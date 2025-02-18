package eva3660

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import groovy.json.JsonSlurper
import org.apache.commons.lang.SerializationUtils
import org.bson.BsonSerializationException
import org.bson.Document
import org.bson.conversions.Bson
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import uk.ac.ebi.eva.commons.models.data.Variant
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo

import java.nio.file.Paths
import java.util.regex.Pattern
import java.util.stream.Collectors

import static org.springframework.data.mongodb.core.query.Criteria.where

@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class RemediationApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(RemediationApplication.class)
    private static long counter = 0

    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String FILES_COLLECTION = "files_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    public static final String FILES_COLLECTION_STUDY_ID_KEY = "sid"
    public static final String FILES_COLLECTION_FILE_ID_KEY = "fid"

    // Search for IDs of indels
    // Ignore non-ACGTN alleles (*, <>), but do include alleles longer than 50 bases (encoded ids)
    public static final String REGEX_PATTERN = "" +
            "([^_<>*\\s]_" +    // ref exactly 1 char, no alt
            "\$)" +             // anchor on end of string
            "|" +               // OR
            "(__[^_<>*\\s]" +   // no ref, alt exactly 1 char
            "\$)" +             // anchor on end of string
            "|" +               // OR
            "([^_<>*\\s]{2,}" + // ref at least 2 chars
            "_[^_<>*\\s]*" +    // alt can be any length
            "\$)" +             // anchor on end of string
            "|" +               // OR
            "([^_<>*\\s]*" +    // ref any length
            "_[^_<>*\\s]{2,}" + // alt at least 2 chars
            "\$)"               // anchor on end of string

    @Autowired
    MongoTemplate mongoTemplate

    private static Map<String, Integer> sidFidNumberOfSamplesMap = new HashMap<>()
    private static VariantStatsProcessor variantStatsProcessor = new VariantStatsProcessor()
    private static NormalisationProcessor normaliser = null
    private static ContigRenamingProcessor contigRenamer = null
    private static String workingDir = ""
    private static String dbName = ""
    private static String fastaDir = ""

    @Override
    void run(String... args) throws Exception {
        workingDir = args[0]
        dbName = args[1]
        fastaDir = args[2]

        def (fastaPath, assemblyReportPath) = getFastaAndReportPaths(fastaDir, dbName)
        normaliser = new NormalisationProcessor(fastaPath)
        contigRenamer = new ContigRenamingProcessor(assemblyReportPath, dbName)

        populateFilesIdAndNumberOfSamplesMap()

        // workaround to not saving a field by the name _class (contains the java class name) in the document
        MappingMongoConverter converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(VARIANTS_COLLECTION)

        Query regexQuery = new Query(where("_id").regex(REGEX_PATTERN))
        logger.info("{}", regexQuery)
        def mongoCursor = variantsColl.find(regexQuery.getQueryObject()).noCursorTimeout(true).iterator()

        // Iterate through each variant one by one
        while (mongoCursor.hasNext()) {
            counter++
            if (counter % 1000 == 0) {
                logger.info("Processed {} variants", counter)
            }
            Document candidateDocument = mongoCursor.next()
            VariantDocument originalVariant
            try {
                // read the variant as a VariantDocument
                originalVariant = mongoTemplate.getConverter().read(VariantDocument.class, candidateDocument)
            } catch (Exception e) {
                logger.error("Exception while converting Bson document to Variant Document with _id: {} " +
                        "chr {} start {} ref {} alt {}. Exception: {}", candidateDocument.get("_id"),
                        candidateDocument.get("chr"), candidateDocument.get("start"), candidateDocument.get("ref"),
                        candidateDocument.get("alt"), e.getMessage())
                continue
            }

            // filter out false positives
            if (!isCandidateForNormalisation(originalVariant)) {
                continue
            }
            logger.info("Variant might require normalisation. variant_id: {}, variant_ref: {}, variant_alt: {}",
                    originalVariant.getId(), originalVariant.getReference(), originalVariant.getAlternate())

            // Each source file has its own list of secondary alternates, these need to be normalised independently.
            // This also means that potentially more than one normalised document might be generated from one original
            // document, as the final normalised ID might differ depending on the secondary alternates.
            Map<String, VariantDocument> variantIdToDocument = [:]
            for (VariantSourceEntryMongo variantSource : originalVariant.getVariantSources()) {
                remediateVariantForFile(originalVariant, variantSource, variantIdToDocument)
            }
            if (variantIdToDocument.isEmpty()) {
                logger.info("No remediated documents, skipping")
                continue
            }

            // If there is only one new document and its id is identical to the original, then nothing has changed
            // from normalisation and we can skip
            String originalId = originalVariant.getId()
            if (variantIdToDocument.size() == 1) {
                String newId = variantIdToDocument.keySet().iterator().next()
                if (originalId.equals(newId)) {
                    logger.info("Variant did not change from normalisation, skipping")
                    continue
                }
            }

            // At this point, we have one or more variant IDs (which may be same as the original ID) and associated
            // split documents. Accumulate write operations to be committed at the end.
            BulkOperations variantOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, VARIANTS_COLLECTION)

            // Then insert new documents, merging with colliding IDs as needed
            boolean executeRemediation = true
            for (String newId : variantIdToDocument.keySet()) {
                logger.info("Processing new ID : {}", newId)
                boolean executeForThisId = insertOrMergeRemediateVariant(originalId, newId, variantIdToDocument[newId],
                        variantOps)
                executeRemediation = executeRemediation && executeForThisId
            }

            // At this point, if merges for all split documents are unambiguous, execute the operations
            // and remediate annotations
            if (executeRemediation) {
                logger.info("Executing remediation - delete original variant: {}", originalId)
                mongoTemplate.remove(Query.query(Criteria.where("_id").is(originalId)), VARIANTS_COLLECTION)
                variantOps.execute()
                remediateAnnotations(originalId, variantIdToDocument.keySet())
            }
        }

        // Finished processing
        System.exit(0)
    }

    void remediateVariantForFile(VariantDocument originalVariant, VariantSourceEntryMongo variantSource,
                                 Map<String, VariantDocument> variantIdToDocument) {
        String fid = variantSource.getFileId()
        String sid = variantSource.getStudyId()
        List<String> secondaryAlternates = variantSource.getAlternates() as List<String>
        if (secondaryAlternates == null) {
            secondaryAlternates = []
        }

        // Convert the contig name to INSDC
        // Used ONLY in normalisation, the original contig name will be retained in the DB
        String insdcContig
        try {
            insdcContig = contigRenamer.getInsdcAccession(originalVariant.getChromosome())
        } catch (ContigNotFoundException e) {
            // Log and bypass these errors
            logger.error("Bypassing ContigNotFoundException {}", e.getMessage())
            return
        }

        // Normalise all alleles and truncate common leading context allele if present
        ValuesForNormalisation normalisedValues = normaliser.normaliseAndTruncate(insdcContig,
                new ValuesForNormalisation(originalVariant.getStart(), originalVariant.getEnd(), originalVariant.getLength(),
                        originalVariant.getReference(), originalVariant.getAlternate(), secondaryAlternates))

        // Create new variantId and file subdocument
        String remediatedId = VariantDocument.buildVariantId(originalVariant.getChromosome(),
                normalisedValues.getStart(), normalisedValues.getReference(), normalisedValues.getAlternate())
        VariantSourceEntryMongo remediatedFile = new VariantSourceEntryMongo(fid, sid,
                normalisedValues.getSecondaryAlternates() as String[],
                (BasicDBObject) variantSource.getAttrs(), variantSource.getFormat(),
                (BasicDBObject) variantSource.getSampleData())

        if (variantIdToDocument.containsKey(remediatedId)) {
            // Need to add this fid/sid's files to the variant already in the map
            VariantDocument variantInMap = variantIdToDocument[remediatedId]
            Set<VariantSourceEntryMongo> newSources = variantInMap.getVariantSources()
            newSources.add(remediatedFile)
            variantInMap.setSources(newSources)
        } else {
            // Create a new remediated document, containing just the files with this fid/sid pair
            VariantDocument remediatedVariant = new VariantDocument(
                    originalVariant.getVariantType(), originalVariant.getChromosome(),
                    normalisedValues.start, normalisedValues.end, normalisedValues.length,
                    normalisedValues.reference, normalisedValues.alternate, new HashSet<>(), originalVariant.getIds(),
                    [remediatedFile] as Set
            )
            variantIdToDocument[remediatedId] = remediatedVariant
        }
    }

    /**
     * Does not perform inserts or updates, but adds them to the bulk operations param.
     * Returns true if we can proceed with the merging (i.e. fid/sid is unambiguous), false otherwise.
     */
    boolean insertOrMergeRemediateVariant(String originalId, String remediatedId, VariantDocument remediatedVariant,
                                          BulkOperations variantOps) {
        // check if new id is present in db and get the corresponding variant
        Query idQuery = new Query(where("_id").is(remediatedId))
        VariantDocument variantInDB = mongoTemplate.findOne(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

        // Check if there exists a variant in db that has the same id as newID, OR if the new ID is the same as the
        // original (which will subsequently be removed)
        if (variantInDB == null || remediatedId.equals(originalId)) {
            logger.warn("Variant with id {} not found in DB", remediatedId)
            remediateCaseNoIdCollision(remediatedId, remediatedVariant, variantOps)
            return true
        }

        logger.info("Found existing variant in DB with id: {} {}", remediatedId, variantInDB)
        // variant with new db present, needs to check for merging
        Set<VariantSourceEntity> remediatedVariantFileSet = remediatedVariant.getVariantSources() != null ?
                remediatedVariant.getVariantSources() : new HashSet<VariantSourceEntity>()
        Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                variantInDB.getVariantSources() : new HashSet<VariantSourceEntity>()
        Set<Tuple2> remediatedSidFidPairSet = remediatedVariantFileSet.stream()
                .map(vse -> new Tuple2(vse.getStudyId(), vse.getFileId()))
                .collect(Collectors.toSet())
        Set<Tuple2> variantInDBSidFidPairSet = variantInDBFileSet.stream()
                .map(vse -> new Tuple2(vse.getStudyId(), vse.getFileId()))
                .collect(Collectors.toSet())

        // take the common pairs of sid-fid between the remediated variant and the variant in db
        Set<Tuple2> commonSidFidPairs = new HashSet<>(remediatedSidFidPairSet)
        commonSidFidPairs.retainAll(variantInDBSidFidPairSet)

        if (commonSidFidPairs.isEmpty()) {
            logger.info("No common sid fid entries between remediated variant and variant in DB")
            remediateCaseMergeAllSidFidAreDifferent(variantInDB, remediatedId, remediatedVariant, variantOps)
            return true
        }

        // check if there is any pair of sid and fid from common pair, for which there are more than one entry in files
        // collection
        Map<Tuple2, Integer> result = getSidFidPairNumberOfDocumentsMap(commonSidFidPairs)
        Set<Tuple2> sidFidPairsWithGTOneEntry = result.entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet())
        if (sidFidPairsWithGTOneEntry.isEmpty()) {
            logger.info("All common sid fid entries has only one file entry")
            Set<Tuple2> sidFidPairNotInDB = new HashSet<>(remediatedSidFidPairSet)
            sidFidPairNotInDB.removeAll(commonSidFidPairs)
            remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, sidFidPairNotInDB, remediatedId, remediatedVariant, variantOps)
            return true
        }

        logger.info("can't merge as sid fid common pair has more than 1 entry in file")
        remediateCaseCantMerge(originalId, workingDir, sidFidPairsWithGTOneEntry, dbName)
        return false
    }

    void remediateCaseNoIdCollision(String newId, VariantDocument remediatedVariant, BulkOperations variantOps) {
        // insert updated variant and delete the existing one
        logger.info("case no id collision - insert new variant: {}", newId)
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(remediatedVariant.getReference(),
                remediatedVariant.getAlternate(), [] as Set, remediatedVariant.getVariantSources(),
                sidFidNumberOfSamplesMap)
        remediatedVariant.setStats(variantStats)
        variantOps.insert(remediatedVariant)
    }

    void remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, String newId,
                                                 VariantDocument remediatedVariant, BulkOperations variantOps) {
        logger.info("case merge all sid fid are different - processing variant: {}", newId)

        // Add all files within remediatedVariant
        Set<VariantSourceEntryMongo> remediatedFiles = remediatedVariant.getVariantSources()

        // Recompute stats
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(remediatedVariant.getReference(),
                remediatedVariant.getAlternate(), variantInDB.getVariantSources(), remediatedFiles,
                sidFidNumberOfSamplesMap)

        Update updateOp = new Update()
        updateOp.push("files", new Document("\$each", remediatedFiles.stream()
                .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                .collect(Collectors.toList())))
        updateOp.set("st", variantStats.stream()
                .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                .collect(Collectors.toList()))

        logger.info("case merge all sid fid are different - updates: {}", updateOp)
        variantOps.updateOne(new Query(where("_id").is(newId)), updateOp)
    }

    void remediateCaseCantMerge(String originalId, String workingDir, Set<Tuple2> sidFidPairsWithGtOneEntry, String dbName) {
        logger.info("case can't merge variant - processing variant: {}", originalId)

        String nmcDirPath = Paths.get(workingDir, "non_merged_candidates").toString()
        File nmcDir = new File(nmcDirPath)
        if (!nmcDir.exists()) {
            nmcDir.mkdirs()
        }
        String nmcFilePath = Paths.get(nmcDirPath, dbName + ".txt").toString()
        try (BufferedWriter nmcFile = new BufferedWriter(new FileWriter(nmcFilePath, true))) {
            for (Tuple2<String, String> p : sidFidPairsWithGtOneEntry) {
                nmcFile.write(p.v1 + "," + p.v2 + "," + originalId + "\n")
            }
        } catch (IOException e) {
            logger.error("error writing case variant can't be merged in the file:  {}", originalId)
        }
    }

    void remediateCaseMergeAllCommonSidFidHasOneFile(VariantDocument variantInDB,
                                                     Set<Tuple2> sidFidPairsNotInDB, String newId,
                                                     VariantDocument remediatedVariant, BulkOperations variantOps) {
        logger.info("case merge all common sid fid has one file - processing variant: {}", newId)

        // Add only files that are not already in variantInDB
        Set<VariantSourceEntryMongo> remediatedFiles = remediatedVariant.getVariantSources().stream()
                .filter(vse -> sidFidPairsNotInDB.contains(new Tuple2(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())

        // Recompute stats
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), remediatedFiles, sidFidNumberOfSamplesMap)

        Update updateOp = new Update()
        updateOp.push("files", new Document("\$each", remediatedFiles.stream()
                .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                .collect(Collectors.toList())))
        updateOp.set("st", variantStats.stream()
                .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                .collect(Collectors.toList()))

        logger.info("case merge all common sid fid has one file - updates: {}", updateOp)
        variantOps.updateOne(new Query(where("_id").is(newId)), updateOp)
    }

    void remediateAnnotations(String originalVariantId, Set<String> newVariantIds) {
        // Fix associated annotations - remove the original one and insert remediated one if not present
        String escapedOriginalVariantId = Pattern.quote(originalVariantId)
        Query originalAnnotationQuery = new Query(where("_id").regex("^" + escapedOriginalVariantId + ".*"))
        boolean removeOriginalAnnotation = !newVariantIds.contains(originalVariantId)
        boolean hasWrites = false

        try {
            BulkOperations annotationOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, ANNOTATIONS_COLLECTION)
            List<Document> originalAnnotationsSet = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                    .find(originalAnnotationQuery.getQueryObject())
                    .into([])
            for (Document annotation : originalAnnotationsSet) {
                String originalAnnotationId = annotation.get("_id")
                // Insert annotation for each new id
                for (String newVariantId : newVariantIds) {
                    Document newAnnotation = SerializationUtils.clone(annotation)
                    newAnnotation.put("_id", originalAnnotationId.replace(originalVariantId, newVariantId))
                    annotationOps.insert(newAnnotation)
                    hasWrites = true
                }
                // Remove old id, if needed
                if (removeOriginalAnnotation) {
                    annotationOps.remove(Query.query(where("_id").is(originalAnnotationId)))
                }
            }

            // Write everything, ignoring duplicates
            try {
                if (hasWrites) {
                    annotationOps.execute()
                }
            } catch (DuplicateKeyException ex) {
            }
        } catch (BsonSerializationException ex) {
            logger.error("Exception occurred while trying to remediate annotation for variant: {}", originalVariantId)
        }
    }

    private void populateFilesIdAndNumberOfSamplesMap() {
        def projectStage = Aggregates.project(Projections.fields(
                Projections.computed("sid_fid", new Document("\$concat", Arrays.asList("\$sid", "_", "\$fid"))),
                Projections.computed("numOfSamples", new Document("\$size", new Document("\$objectToArray", "\$samp")))
        ))
        def groupStage = Aggregates.group("\$sid_fid",
                Accumulators.sum("totalNumOfSamples", "\$numOfSamples"),
                Accumulators.sum("count", 1))

        def filterStage = Aggregates.match(Filters.eq("count", 1))

        sidFidNumberOfSamplesMap = mongoTemplate.getCollection(FILES_COLLECTION)
                .aggregate(Arrays.asList(projectStage, groupStage, filterStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap({ doc -> doc.getString("_id") }, { doc -> doc.getInteger("totalNumOfSamples") }))
    }


    Map<Tuple2, Integer> getSidFidPairNumberOfDocumentsMap(Set<Tuple2> commonSidFidPairs) {
        List<Bson> filterConditions = new ArrayList<>()
        for (Tuple2 sidFidPair : commonSidFidPairs) {
            filterConditions.add(Filters.and(Filters.eq(FILES_COLLECTION_STUDY_ID_KEY, sidFidPair.v1),
                    Filters.eq(FILES_COLLECTION_FILE_ID_KEY, sidFidPair.v2)))
        }
        Bson filter = Filters.or(filterConditions)

        Map<Tuple2, Integer> sidFidPairCountMap = mongoTemplate.getCollection(FILES_COLLECTION).find(filter)
                .into(new ArrayList<>()).stream()
                .map(doc -> new Tuple2(doc.get(FILES_COLLECTION_STUDY_ID_KEY),
                        doc.get(FILES_COLLECTION_FILE_ID_KEY)))
                .collect(Collectors.toMap(Tuple2 -> Tuple2, count -> 1, Integer::sum))

        return sidFidPairCountMap
    }

    boolean isCandidateForNormalisation(VariantDocument variant) {
        return variant.getVariantType() != Variant.VariantType.SNV
    }

    static Tuple2 getFastaAndReportPaths(String fastaDir, String dbName) {
        // Get path to FASTA file and assembly report based on dbName
        // Assembly code is allowed to have underscores, e.g. eva_bbubalis_uoa_wb_1
        def dbNameParts = dbName.split("_")
        String taxonomyCode = dbNameParts[1]
        String assemblyCode = String.join("_", dbNameParts[2..-1])
        String scientificName = null
        String assemblyAccession = null

        JsonSlurper jsonParser = new JsonSlurper()
        def results = jsonParser.parse(new URL("https://www.ebi.ac.uk/eva/webservices/rest/v1/meta/species/list"))["response"]["result"][0]
        for (Map<String, String> result: results) {
            if (result["assemblyCode"] == assemblyCode && result["taxonomyCode"] == taxonomyCode) {
                // Choose most recent patch when multiple assemblies have the same assembly code
                if (assemblyAccession == null || result["assemblyAccession"] > assemblyAccession) {
                    scientificName = result["taxonomyScientificName"].toLowerCase().replace(" ", "_")
                    assemblyAccession = result["assemblyAccession"]
                }
            }
        }
        if (scientificName == null || assemblyAccession == null) {
            throw new RuntimeException("Could not determine scientific name and assembly accession for db " + dbName)
        }

        // See here: https://github.com/EBIvariation/eva-common-pyutils/blob/master/ebi_eva_common_pyutils/reference/assembly.py#L61
        return new Tuple2(Paths.get(fastaDir, scientificName, assemblyAccession, assemblyAccession + ".fa"),
                Paths.get(fastaDir, scientificName, assemblyAccession, assemblyAccession + "_assembly_report.txt"))
    }

}
