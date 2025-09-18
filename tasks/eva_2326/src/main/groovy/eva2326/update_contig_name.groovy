package eva2326

import com.mongodb.client.model.*
import groovy.cli.picocli.CliBuilder
import groovy.json.JsonSlurper
import org.bson.BsonSerializationException
import org.bson.Document
import org.bson.conversions.Bson
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.util.Pair
import uk.ac.ebi.eva.commons.models.data.Variant
import uk.ac.ebi.eva.commons.models.data.VariantSourceEntity
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.HgvsMongo

import java.nio.file.Paths
import java.util.regex.Pattern
import java.util.stream.Collectors

import static org.springframework.data.mongodb.core.query.Criteria.where

def cli = new CliBuilder()
cli.workingDir(args: 1, "Path to the working directory where processing files will be kept", required: true)
cli.envPropertiesFile(args: 1, "Properties file with db details to use for update", required: true)
cli.dbName(args: 1, "Database name that needs to be updated", required: true)
cli.assemblyReportDir(args: 1, "Path to the root of the directory containing FASTA files", required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}


// Run the remediation application
new SpringApplicationBuilder(UpdateContigApplication.class).properties([
        'spring.config.location'      : options.envPropertiesFile,
        'spring.data.mongodb.database': options.dbName])
        .run(options.workingDir, options.dbName, options.assemblyReportDir)


@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class UpdateContigApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(UpdateContigApplication.class)
    private static int BATCH_SIZE = 1000
    private static long BATCH_COUNT = 0
    private static long TOTAL_COUNTS = 0
    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String FILES_COLLECTION = "files_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    public static final String FILES_COLLECTION_STUDY_ID_KEY = "sid"
    public static final String FILES_COLLECTION_FILE_ID_KEY = "fid"

    @Autowired
    MongoTemplate mongoTemplate

    MappingMongoConverter converter
    ContigRenamingProcessor contigRenamer
    String variantsWithIssuesFilePath
    String annotationRemediationFilePath

    private static Map<String, Integer> sidFidNumberOfSamplesMap = new HashMap<>()
    private static VariantStatsProcessor variantStatsProcessor = new VariantStatsProcessor()

    @Override
    void run(String... args) throws Exception {
        String workingDir = args[0]
        String dbName = args[1]
        String assemblyReportDir = args[2]

        // create a dir to store variants that could not be processed due to various reasons
        String variantsWithIssuesDirPath = Paths.get(workingDir, "Variants_With_Issues").toString()
        File variantsWithIssuesDir = new File(variantsWithIssuesDirPath)
        if (!variantsWithIssuesDir.exists()) {
            variantsWithIssuesDir.mkdirs()
        }

        variantsWithIssuesFilePath = Paths.get(variantsWithIssuesDirPath, dbName + ".txt").toString()

        // create a dir to store variant's ids/info for annotation's remediation
        String annotationsRemediationDirPath = Paths.get(workingDir, "Annotations_Remediation").toString()
        File annotationsRemediationDir = new File(annotationsRemediationDirPath)
        if (!annotationsRemediationDir.exists()) {
            annotationsRemediationDir.mkdirs()
        }
        annotationRemediationFilePath = Paths.get(annotationsRemediationDirPath, dbName + ".txt").toString()

        // populate sidFidNumberOfSamplesMap
        populateFilesIdAndNumberOfSamplesMap()

        // workaround to not saving a field by the name _class (contains the java class name) in the document
        converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        def (fastaPath, assemblyReportPath) = getFastaAndReportPaths(assemblyReportDir, dbName)

        // load assembly report for contig mapping
        contigRenamer = new ContigRenamingProcessor(assemblyReportPath, dbName)

        // Obtain a MongoCursor to iterate through documents
        def mongoCursor = mongoTemplate.getCollection(VARIANTS_COLLECTION)
                .find()
                .noCursorTimeout(true)
                .batchSize(BATCH_SIZE)
                .iterator()

        List<VariantDocument> variantBatch = new ArrayList<>()

        while (mongoCursor.hasNext()) {
            Document orgDocument = mongoCursor.next()
            VariantDocument orgVariant
            try {
                orgVariant = mongoTemplate.getConverter().read(VariantDocument.class, orgDocument)
            } catch (Exception e) {
                logger.error("Exception while converting Bson document to Variant Document with _id: {} " +
                        "chr {} start {} ref {} alt {}. Exception: {}", orgDocument.get("_id"),
                        orgDocument.get("chr"), orgDocument.get("start"), orgDocument.get("ref"),
                        orgDocument.get("alt"), e.getMessage())

                storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgDocument.get("_id"), "", "Exception converting Bson Document to Variant Document")
                orgVariant = null
            }

            if (orgVariant != null) {
                variantBatch.add(orgVariant)
                BATCH_COUNT++
            }

            if (BATCH_COUNT >= BATCH_SIZE) {
                processVariantBatch(variantBatch)
                TOTAL_COUNTS += BATCH_COUNT
                logger.info("Total document processed till now: {}", TOTAL_COUNTS)
                variantBatch.clear()
                BATCH_COUNT = 0
            }
        }

        if (variantBatch.size() > 0) {
            processVariantBatch(variantBatch)
            TOTAL_COUNTS += BATCH_COUNT
            logger.info("Total document processed till now: {}", TOTAL_COUNTS)
            variantBatch.clear()
            BATCH_COUNT = 0
        }

        // Finished processing
        System.exit(0)
    }

    void processVariantBatch(List<VariantDocument> variantDocumentList) {

        while (!variantDocumentList.isEmpty()) {
            Set<String> idProcessed = new HashSet<>()
            List<VariantDocument> processInNextLoop = new ArrayList<>()


            // create a map of original variant id and its INSDC accession chromosome
            Map<String, String> orgVariantInsdcChrMap = variantDocumentList.stream()
                    .map(orgVariant -> {
                        String orgChromosome = orgVariant.getChromosome()
                        try {
                            return new AbstractMap.SimpleEntry<>(orgVariant.getId(),
                                    contigRenamer.getInsdcAccession(orgChromosome))
                        } catch (ContigNotFoundException e) {
                            UpdateContigApplication.logger.error("Could not get INSDC accession for variant {} with chromosome {}. Exception Message: {}",
                                    orgVariant.getId(), orgChromosome, e.getMessage())
                            storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgVariant.getId(), "",
                                    "Could not get INSDC accession for Chromosome " + orgChromosome)
                            return null
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))

            // create a map of original variant id and its new id with INSDC accession chromosome
            Map<String, String> orgVariantNewIdMap = variantDocumentList.stream()
                    .filter(orgVariant -> orgVariantInsdcChrMap.get(orgVariant.getId()) != null)
                    .collect(Collectors.toMap(
                            orgVariant -> orgVariant.getId(),
                            orgVariant -> {
                                String updatedChromosome = orgVariantInsdcChrMap.get(orgVariant.getId())
                                return VariantDocument.buildVariantId(
                                        updatedChromosome,
                                        orgVariant.getStart(),
                                        orgVariant.getReference(),
                                        orgVariant.getAlternate()
                                )
                            }
                    ))

            // get all the new ids
            List<String> newIdsList = orgVariantNewIdMap.values().stream()
                    .filter(val -> val != null)
                    .collect(Collectors.toList())

            // get a list of variants already in db with new ids
            List<VariantDocument> variantsInDBList = getVariantsInDB(newIdsList)
            // create a map of new id and its associated variant in db
            Map<String, VariantDocument> variantsInDBMap = variantsInDBList.stream()
                    .collect(Collectors.toMap(variant -> variant.getId(), variant -> variant))

            List<String> documentsToDeleteIdList = new ArrayList<>()
            List<VariantDocument> documentsToInsertList = new ArrayList<>()
            List<WriteModel<Document>> bulkUpdateOperations = new ArrayList<>()
            Set<String> annotationsToBeUpdated = new HashSet<>()

            // process each variant in the batch
            for (VariantDocument orgVariant : variantDocumentList) {
                String orgChromosome = orgVariant.getChromosome()
                String updatedChromosome = orgVariantInsdcChrMap.get(orgVariant.getId())

                // If could not find insdc chromosome or the variant already has insdc chromosome, no processing required
                if (updatedChromosome == null || orgChromosome == updatedChromosome) {
                    continue
                }

                String newId = orgVariantNewIdMap.get(orgVariant.getId())

                // check if id already processed in the loop (in mem id collision)
                if (idProcessed.contains(newId)) {
                    processInNextLoop.add(orgVariant)
                    continue
                }

                // check if there exists a variant in db with new id
                if (!variantsInDBMap.containsKey(newId)) {
                    // case no id collision
                    VariantDocument updatedVariant = remediateCaseNoIdCollision(orgVariant, newId, updatedChromosome)
                    documentsToDeleteIdList.add(orgVariant.getId())
                    documentsToInsertList.add(updatedVariant)

                    annotationsToBeUpdated.add(orgVariant.getId())
                    idProcessed.add(newId)

                    continue
                }

                VariantDocument variantInDB = variantsInDBMap.get(newId)
                logger.info("Found existing variant in DB with id: {} {}", newId, variantInDB)

                // variant with new db present, needs to check for merging
                Set<VariantSourceEntity> orgVariantFileSet = orgVariant.getVariantSources() != null ?
                        orgVariant.getVariantSources() : new HashSet<>()
                Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                        variantInDB.getVariantSources() : new HashSet<>()
                Set<Pair> orgSidFidPairSet = orgVariantFileSet.stream()
                        .filter(vse -> vse.getStudyId() != null && vse.getFileId() != null)
                        .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                        .collect(Collectors.toSet())
                Set<Pair> variantInDBSidFidPairSet = variantInDBFileSet.stream()
                        .filter(vse -> vse.getStudyId() != null && vse.getFileId() != null)
                        .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                        .collect(Collectors.toSet())

                // take the common pair of sid-fid between the org variant and the variant in db
                Set<Pair> commonSidFidPairs = new HashSet<>(orgSidFidPairSet)
                commonSidFidPairs.retainAll(variantInDBSidFidPairSet)

                if (commonSidFidPairs.isEmpty()) {
                    logger.info("No common sid fid entries between org variant and variant in DB")
                    if (orgVariant.getVariantSources() != null && variantInDB.getVariantSources() != null) {
                        List<Bson> updateOperations = remediateCaseMergeAllSidFidAreDifferent(variantInDB, orgVariant, newId, updatedChromosome)
                        // add updates to bulk updates and existing variant to delete list
                        bulkUpdateOperations.add(new UpdateOneModel<>(Filters.eq("_id", newId), Updates.combine(updateOperations)))
                        documentsToDeleteIdList.add(orgVariant.getId())

                        annotationsToBeUpdated.add(orgVariant.getId())
                        idProcessed.add(newId)
                    } else {
                        if (orgVariant.getVariantSources() == null) {
                            logger.error("Variant does not have variant sources (files) field. variant: "
                                    + orgVariant.getId() + " " + orgVariant.getChromosome()
                                    + " " + orgVariant.getReference() + " " + orgVariant.getAlternate()
                                    + " " + orgVariant.getStart() + " " + orgVariant.getEnd())
                            storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgVariant.getId(), "",
                                    "Variant Does not have variant sources (files) field " + orgChromosome)
                        }
                        if (variantInDB.getVariantSources() == null) {
                            logger.error("VariantInDB does not have variant sources (files) field. variant: "
                                    + variantInDB.getId() + " " + variantInDB.getChromosome()
                                    + " " + variantInDB.getReference() + " " + variantInDB.getAlternate()
                                    + " " + variantInDB.getStart() + " " + variantInDB.getEnd())
                            storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, variantInDB.getId(), "",
                                    "VariantInDB does not have variant sources (files) field. " + orgChromosome)
                        }
                    }

                    continue
                }

                // check if there is any pair of sid and fid from common pairs, for which there are more than one entry in files collection
                Map<Pair, Integer> result = getSidFidPairNumberOfDocumentsMap(commonSidFidPairs)
                Set<Pair> sidFidPairsWithGTOneEntry = result.entrySet().stream()
                        .filter(entry -> entry.getValue() > 1)
                        .map(entry -> entry.getKey())
                        .collect(Collectors.toSet())
                if (sidFidPairsWithGTOneEntry.isEmpty()) {
                    logger.info("All common sid fid entries has only one file entry")
                    if(orgVariant.getVariantSources() != null && variantInDB.getVariantSources() != null){
                        Set<Pair> sidFidPairNotInDB = new HashSet<>(orgSidFidPairSet)
                        sidFidPairNotInDB.removeAll(commonSidFidPairs)
                        List<Bson> updateOperations = remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, orgVariant, sidFidPairNotInDB, newId, updatedChromosome)
                        // add updates to bulk updates and existing variant to delete list
                        bulkUpdateOperations.add(new UpdateOneModel<>(Filters.eq("_id", newId), Updates.combine(updateOperations)))
                        documentsToDeleteIdList.add(orgVariant.getId())

                        annotationsToBeUpdated.add(orgVariant.getId())
                        idProcessed.add(newId)
                    }else{
                        if (orgVariant.getVariantSources() == null) {
                            logger.error("Variant does not have variant sources (files) field. variant: "
                                    + orgVariant.getId() + " " + orgVariant.getChromosome()
                                    + " " + orgVariant.getReference() + " " + orgVariant.getAlternate()
                                    + " " + orgVariant.getStart() + " " + orgVariant.getEnd())
                            storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgVariant.getId(), "",
                                    "Variant Does not have variant sources (files) field " + orgChromosome)
                        }
                        if (variantInDB.getVariantSources() == null) {
                            logger.error("VariantInDB does not have variant sources (files) field. variant: "
                                    + variantInDB.getId() + " " + variantInDB.getChromosome()
                                    + " " + variantInDB.getReference() + " " + variantInDB.getAlternate()
                                    + " " + variantInDB.getStart() + " " + variantInDB.getEnd())
                            storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, variantInDB.getId(), "",
                                    "VariantInDB does not have variant sources (files) field. " + orgChromosome)
                        }
                    }


                    continue
                }

                logger.info("can't merge as sid fid common pair has more than 1 entry in file")
                storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgVariant.getId(), variantInDB.getId(), "Can't merge as sid fid common pair has more than 1 entry in file")

            }

            // delete existing variants
            if (!documentsToDeleteIdList.isEmpty()) {
                mongoTemplate.remove(Query.query(Criteria.where("_id").in(documentsToDeleteIdList)), VARIANTS_COLLECTION)
            }

            // insert the updated variants
            if (!documentsToInsertList.isEmpty()) {
                List<Document> documentsToInsert = documentsToInsertList.stream()
                        .map(variantDocument -> mongoTemplate.getConverter().convertToMongoType(variantDocument))
                        .collect(Collectors.toList())
                mongoTemplate.getCollection(VARIANTS_COLLECTION).insertMany(documentsToInsert)
            }

            // perform the batched update
            if (!bulkUpdateOperations.isEmpty()) {
                mongoTemplate.getCollection(VARIANTS_COLLECTION).bulkWrite(bulkUpdateOperations)
            }

            if (!annotationsToBeUpdated.isEmpty()) {
                orgVariantNewIdMap = orgVariantNewIdMap.findAll { k, v -> annotationsToBeUpdated.contains(k) }
//                remediateAnnotations(orgVariantNewIdMap, orgVariantInsdcChrMap)
                writeVariantsForAnnotationUpdates(annotationRemediationFilePath, orgVariantNewIdMap, orgVariantInsdcChrMap)
            }


            variantDocumentList = processInNextLoop

        }
    }


    void writeVariantsForAnnotationUpdates(String annotationRemediationFilePath, Map<String, String> orgVariantNewIdMap, Map<String, String> orgVariantInsdcChrMap) {
        try (BufferedWriter annotationsRemediationFile = new BufferedWriter(new FileWriter(annotationRemediationFilePath, true))) {
            orgVariantNewIdMap.each { originalId, newId ->
                String insdcChromosome = orgVariantInsdcChrMap.get(originalId)
                annotationsRemediationFile.write("${originalId},${newId},${insdcChromosome}\n")
            }
        } catch (IOException e) {
            logger.error("error storing variant id for remediating annotations in file: {}", annotationRemediationFilePath, e)
        }
    }


    List<VariantDocument> getVariantsInDB(List<String> idList) {
        Query idQuery = new Query(where("_id").in(idList))
        List<VariantDocument> variantInDBList = mongoTemplate.find(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

        return variantInDBList
    }

    void storeVariantsThatCantBeProcessed(String variantsWithIssuesFilePath, String variantId, String newVariantId, String reason) {
        try (BufferedWriter variantsWithIssueFile = new BufferedWriter(new FileWriter(variantsWithIssuesFilePath, true))) {
            variantsWithIssueFile.write(variantId + "," + newVariantId + "," + reason + "\n")
        } catch (IOException e) {
            logger.error("error storing variant id that can't be processed in the file:  {}", variantId)
        }
    }

    VariantDocument remediateCaseNoIdCollision(VariantDocument orgVariant, String newId, String updatedChromosome) {
        Map<String, Set<String>> updatedHgvs = getUpdatedHgvs(orgVariant, updatedChromosome)
        VariantDocument updatedVariant = new VariantDocument(orgVariant.getVariantType(), updatedChromosome,
                orgVariant.getStart(), orgVariant.getEnd(), orgVariant.getLength(), orgVariant.getReference(),
                orgVariant.getAlternate(), updatedHgvs, orgVariant.getIds(), orgVariant.getVariantSources())
        updatedVariant.setStats(orgVariant.getVariantStatsMongo())

        return updatedVariant
    }

    List<Bson> remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, VariantDocument orgVariant, String newId,
                                                       String updatedChromosome) {
        List<Bson> updateOperations = new ArrayList<>()

        Set<VariantSourceEntryMongo> variantSourceFiles = orgVariant.getVariantSources()
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), variantSourceFiles, sidFidNumberOfSamplesMap)

        updateOperations.add(Updates.push("files", new Document("\$each", variantSourceFiles.stream()
                .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                .collect(Collectors.toList()))))
        updateOperations.add(Updates.set("st", variantStats.stream()
                .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                .collect(Collectors.toList())))

        Map<String, Set<String>> updatedHgvs = getUpdatedHgvs(orgVariant, updatedChromosome)
        if (!updatedHgvs.isEmpty()) {
            String updatedHgvsName = updatedHgvs.values().iterator().next()
            boolean hgvsNameAlreadyInDB = variantInDB.getHgvs().stream()
                    .map(hgvs -> hgvs.getName())
                    .anyMatch(name -> name.equals(updatedHgvsName))
            if (!hgvsNameAlreadyInDB) {
                HgvsMongo hgvsMongo = new HgvsMongo(updatedHgvs.keySet().iterator().next(), updatedHgvsName)
                updateOperations.add(Updates.push("hgvs", new Document("\$each", Collections.singletonList(
                        mongoTemplate.getConverter().convertToMongoType(hgvsMongo)))))
            }
        }

        return updateOperations
    }

    List<Bson> remediateCaseMergeAllCommonSidFidHasOneFile(VariantDocument variantInDB,
                                                           VariantDocument orgVariant, Set<Pair> sidFidPairNotInDB,
                                                           String newId, String updatedChromosome) {
        List<Bson> updateOperations = new ArrayList<>()

        Set<VariantSourceEntryMongo> candidateFiles = orgVariant.getVariantSources().stream()
                .filter(vse -> vse.getStudyId() != null && vse.getFileId() != null)
                .filter(vse -> sidFidPairNotInDB.contains(new Pair(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), candidateFiles, sidFidNumberOfSamplesMap)

        updateOperations.add(Updates.push("files", new Document("\$each", candidateFiles.stream()
                .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                .collect(Collectors.toList()))))
        updateOperations.add(Updates.set("st", variantStats.stream()
                .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                .collect(Collectors.toList())))

        Map<String, Set<String>> updatedHgvs = getUpdatedHgvs(orgVariant, updatedChromosome)
        if (!updatedHgvs.isEmpty()) {
            String updatedHgvsName = updatedHgvs.values().iterator().next()
            boolean hgvsNameAlreadyInDB = variantInDB.getHgvs().stream()
                    .map(hgvs -> hgvs.getName())
                    .anyMatch(name -> name.equals(updatedHgvsName))
            if (!hgvsNameAlreadyInDB) {
                HgvsMongo hgvsMongo = new HgvsMongo(updatedHgvs.keySet().iterator().next(), updatedHgvsName)
                updateOperations.add(Updates.push("hgvs", new Document("\$each", Collections.singletonList(
                        mongoTemplate.getConverter().convertToMongoType(hgvsMongo)))))
            }
        }

        return updateOperations
    }

    void remediateAnnotations(Map<String, String> orgVariantNewIdMap, Map<String, String> orgVariantInsdcChrMap) {
        Set<String> idProcessed = new HashSet<>()

        // Escape variant IDs and prepare a combined regex query
        List<Criteria> regexCriteria = new ArrayList<>()
        for (Map.Entry<String, String> entry : orgVariantNewIdMap.entrySet()) {
            regexCriteria.add(Criteria.where("_id").regex("^" + Pattern.quote(entry.getKey()) + ".*"))
            regexCriteria.add(Criteria.where("_id").regex("^" + Pattern.quote(entry.getValue()) + ".*"))
        }

        Query combinedQuery = new Query(new Criteria().orOperator(regexCriteria.toArray(new Criteria[0])))

        try {
            List<Document> annotationsList = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                    .find(combinedQuery.getQueryObject())
                    .into(new ArrayList<>())

            // Group documents by variant ID prefix
            Map<String, Set<Document>> variantIdToDocuments = new HashMap<>()
            for (Document doc : annotationsList) {
                String id = doc.getString("_id")
                for (String variantId : orgVariantNewIdMap.keySet()) {
                    if (id.startsWith(variantId)) {
                        variantIdToDocuments.computeIfAbsent(variantId, k -> new HashSet<>()).add(doc)
                    }
                }
                for (String newVariantId : orgVariantNewIdMap.values()) {
                    if (id.startsWith(newVariantId)) {
                        variantIdToDocuments.computeIfAbsent(newVariantId, k -> new HashSet<>()).add(doc)
                    }
                }
            }

            // Process each old-new ID pair
            for (Map.Entry<String, String> entry : orgVariantNewIdMap.entrySet()) {
                String orgVariantId = entry.getKey()
                String newVariantId = entry.getValue()

                Set<Document> orgAnnotationsSet = variantIdToDocuments.getOrDefault(orgVariantId, Collections.emptySet())
                Set<String> updatedAnnotationIdSet = variantIdToDocuments.getOrDefault(newVariantId, Collections.emptySet())
                        .stream()
                        .map(doc -> doc.getString("_id"))
                        .collect(Collectors.toSet())

                List<Document> toInsert = new ArrayList<>()
                List<String> toDelete = new ArrayList<>()

                for (Document annotation : orgAnnotationsSet) {
                    try {
                        String orgAnnotationId = annotation.getString("_id")
                        String updatedAnnotationId = orgAnnotationId.replace(orgVariantId, newVariantId)
                        if (!updatedAnnotationIdSet.contains(updatedAnnotationId)) {
                            if (!idProcessed.contains(updatedAnnotationId)) {
                                Document updated = new Document(annotation)
                                updated.put("_id", updatedAnnotationId)
                                updated.put("chr", orgVariantInsdcChrMap.get(orgVariantId))

                                toInsert.add(updated)
                                idProcessed.add(updatedAnnotationId)
                            }
                        }

                        toDelete.add(orgAnnotationId)
                    } catch (Exception e) {
                        logger.error("Error processing annotation for original ID {}: {}", orgVariantId, e.getMessage(), e)
                    }
                }

                // Perform bulk insert and delete
                if (!toDelete.isEmpty()) {
                    mongoTemplate.remove(Query.query(Criteria.where("_id").in(toDelete)), ANNOTATIONS_COLLECTION)
                }
                if (!toInsert.isEmpty()) {
                    mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertMany(toInsert)
                }
            }
        } catch (BsonSerializationException ex) {
            logger.error("Exception occurred while trying to update annotations: ", ex.getMessage(), ex)
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

        sidFidNumberOfSamplesMap = mongoTemplate.getCollection(UpdateContigApplication.FILES_COLLECTION)
                .aggregate(Arrays.asList(projectStage, groupStage, filterStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap({ doc -> doc.getString("_id") }, { doc -> doc.getInteger("totalNumOfSamples") }))
    }


    Map<Pair, Integer> getSidFidPairNumberOfDocumentsMap(Set<Pair> commonSidFidPairs) {
        List<Bson> filterConditions = new ArrayList<>()
        for (Pair sidFidPair : commonSidFidPairs) {
            filterConditions.add(Filters.and(Filters.eq(UpdateContigApplication.FILES_COLLECTION_STUDY_ID_KEY, sidFidPair.getFirst()),
                    Filters.eq(UpdateContigApplication.FILES_COLLECTION_FILE_ID_KEY, sidFidPair.getSecond())))
        }
        Bson filter = Filters.or(filterConditions)

        Map<Pair, Integer> sidFidPairCountMap = mongoTemplate.getCollection(FILES_COLLECTION).find(filter)
                .into(new ArrayList<>()).stream()
                .map(doc -> new Pair(doc.get(UpdateContigApplication.FILES_COLLECTION_STUDY_ID_KEY),
                        doc.get(UpdateContigApplication.FILES_COLLECTION_FILE_ID_KEY)))
                .collect(Collectors.toMap(pair -> pair, count -> 1, Integer::sum))

        return sidFidPairCountMap
    }

    Map<String, Set<String>> getUpdatedHgvs(VariantDocument variant, String updatedChromosome) {
        Map<String, Set<String>> hgvs = new HashMap<>()
        if (variant.getVariantType().equals(Variant.VariantType.SNV)) {
            Set<String> hgvsCodes = new HashSet<>()
            hgvsCodes.add(updatedChromosome + ":g." + variant.getStart()
                    + variant.getReference() + ">" + variant.getAlternate())
            hgvs.put("genomic", hgvsCodes)
        }

        return hgvs
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
        for (Map<String, String> result : results) {
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