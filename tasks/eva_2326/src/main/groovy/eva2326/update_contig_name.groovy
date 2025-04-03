package eva2326

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import groovy.cli.picocli.CliBuilder
import org.bson.BsonSerializationException
import org.bson.Document
import org.bson.conversions.Bson
import org.opencb.biodata.models.feature.Genotype
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
import uk.ac.ebi.eva.commons.models.data.VariantStats
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo
import uk.ac.ebi.eva.commons.mongodb.entities.subdocuments.HgvsMongo
import uk.ac.ebi.eva.pipeline.io.AssemblyReportReader
import uk.ac.ebi.eva.pipeline.io.contig.ContigMapping
import uk.ac.ebi.eva.pipeline.io.contig.ContigSynonyms

import java.nio.file.Paths
import java.util.regex.Pattern
import java.util.stream.Collectors

import static org.springframework.data.mongodb.core.query.Criteria.where

def cli = new CliBuilder()
cli.workingDir(args: 1, "Path to the working directory where processing files will be kept", required: true)
cli.envPropertiesFile(args: 1, "Properties file with db details to use for update", required: true)
cli.dbName(args: 1, "Database name that needs to be updated", required: true)
cli.assemblyReportPath(args: 1, "Path to the assembly report", required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}


// Run the remediation application
new SpringApplicationBuilder(UpdateContigApplication.class).properties([
        'spring.config.location'      : options.envPropertiesFile,
        'spring.data.mongodb.database': options.dbName])
        .run(options.workingDir, options.dbName, options.assemblyReportPath)


@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class UpdateContigApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(UpdateContigApplication.class)
    private static long counter = 0

    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String FILES_COLLECTION = "files_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    public static final String FILES_COLLECTION_STUDY_ID_KEY = "sid"
    public static final String FILES_COLLECTION_FILE_ID_KEY = "fid"

    @Autowired
    MongoTemplate mongoTemplate

    private static Map<String, Integer> sidFidNumberOfSamplesMap = new HashMap<>()
    private static VariantStatsProcessor variantStatsProcessor = new VariantStatsProcessor()

    @Override
    void run(String... args) throws Exception {
        String workingDir = args[0]
        String dbName = args[1]
        String assemblyReportPath = args[2]

        // create a dir to store variants that could not be processed due to various reasons
        String variantsWithIssuesDirPath = Paths.get(workingDir, "Variants_With_Issues").toString()
        File variantsWithIssuesDir = new File(variantsWithIssuesDirPath)
        if (!variantsWithIssuesDir.exists()) {
            variantsWithIssuesDir.mkdirs()
        }

        String variantsWithIssuesFilePath = Paths.get(variantsWithIssuesDirPath, dbName + ".txt").toString()

        // populate sidFidNumberOfSamplesMap
        populateFilesIdAndNumberOfSamplesMap()

        // workaround to not saving a field by the name _class (contains the java class name) in the document
        MappingMongoConverter converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        // load assembly report for contig mapping
        ContigMapping contigMapping = new ContigMapping(new AssemblyReportReader(assemblyReportPath))

        // Obtain a MongoCursor to iterate through documents
        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(VARIANTS_COLLECTION)
        def mongoCursor = variantsColl.find().noCursorTimeout(true).iterator()

        // Iterate through each variant one by one
        while (mongoCursor.hasNext()) {
            counter++
            if ((counter / 100000) == 0) {
                logger.info("Processed Variants: {}", counter)
            }

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
                continue
            }

            // get INSDC contig
            String orgChromosome = orgVariant.getChromosome()
            String updatedChromosome
            StringBuilder reason = new StringBuilder()
            ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(orgChromosome)
            if (contigMapping.isGenbankReplacementPossible(orgChromosome, contigSynonyms, reason)) {
                updatedChromosome = contigSynonyms.getGenBank()
            } else {
                logger.error("Could not get INSDC accession for variant {} with chromosome. Reason: {}", orgVariant.getId(),
                        orgVariant.getChromosome(), reason)

                storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgVariant.getId(), "", "Could not get INSDC accession for Chromosome " + orgChromosome)
                continue
            }

            // variant already has INSDC accession, no processing required
            if (orgChromosome == updatedChromosome) {
                continue
            }

            // create new id of variant with updated chromosome
            String newId = VariantDocument.buildVariantId(updatedChromosome, orgVariant.getStart(),
                    orgVariant.getReference(), orgVariant.getAlternate())
            // check if new id is present in db and get the corresponding variant
            Query idQuery = new Query(where("_id").is(newId))
            VariantDocument variantInDB = mongoTemplate.findOne(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

            // Check if there exists a variant in db that has the same id as newID and process accordingly

            // No Variant with new id found
            if (variantInDB == null) {
                remediateCaseNoIdCollision(orgVariant, newId, updatedChromosome)
                continue
            }

            logger.info("Found existing variant in DB with id: {} {}", newId, variantInDB)
            // variant with new db present, needs to check for merging
            Set<VariantSourceEntity> orgVariantFileSet = orgVariant.getVariantSources() != null ?
                    orgVariant.getVariantSources() : new HashSet<>()
            Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                    variantInDB.getVariantSources() : new HashSet<>()
            Set<Pair> orgSidFidPairSet = orgVariantFileSet.stream()
                    .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())
            Set<Pair> variantInDBSidFidPairSet = variantInDBFileSet.stream()
                    .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())

            // take the common pair of sid-fid between the org variant and the variant in db
            Set<Pair> commonSidFidPairs = new HashSet<>(orgSidFidPairSet)
            commonSidFidPairs.retainAll(variantInDBSidFidPairSet)

            if (commonSidFidPairs.isEmpty()) {
                logger.info("No common sid fid entries between org variant and variant in DB")
                remediateCaseMergeAllSidFidAreDifferent(variantInDB, orgVariant, newId, updatedChromosome)
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
                Set<Pair> sidFidPairNotInDB = new HashSet<>(orgSidFidPairSet)
                sidFidPairNotInDB.removeAll(commonSidFidPairs)
                remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, orgVariant, sidFidPairNotInDB, newId, updatedChromosome)
                continue
            }

            logger.info("can't merge as sid fid common pair has more than 1 entry in file")
            storeVariantsThatCantBeProcessed(variantsWithIssuesFilePath, orgVariant.getId(), variantInDB.getId(), "Can't merge as sid fid common pair has more than 1 entry in file")
        }

        logger.info("Processed Variants: {}", counter)

        // Finished processing
        System.exit(0)
    }

    void storeVariantsThatCantBeProcessed(String variantsWithIssuesFilePath, String variantId, String newVariantId, String reason) {
        try (BufferedWriter variantsWithIssueFile = new BufferedWriter(new FileWriter(variantsWithIssuesFilePath, true))) {
            variantsWithIssueFile.write(variantId + "," + newVariantId + "," + reason + "\n")
        } catch (IOException e) {
            logger.error("error storing variant id that can't be processed in the file:  {}", variantId)
        }
    }

    void remediateCaseNoIdCollision(VariantDocument orgVariant, String newId, String updatedChromosome) {
        Map<String, Set<String>> updatedHgvs = getUpdatedHgvs(orgVariant, updatedChromosome)
        Set<VariantSourceEntryMongo> updatedSourceEntries = orgVariant.getVariantSources().stream()
                .peek(vse -> {
                    if (vse.getAttrs() != null) {
                        vse.getAttrs().append("CHR", updatedChromosome);
                    }
                })
                .collect(Collectors.toSet())

        VariantDocument updatedVariant = new VariantDocument(orgVariant.getVariantType(), updatedChromosome,
                orgVariant.getStart(), orgVariant.getEnd(), orgVariant.getLength(), orgVariant.getReference(),
                orgVariant.getAlternate(), updatedHgvs, orgVariant.getIds(), updatedSourceEntries)
        updatedVariant.setStats(orgVariant.getVariantStatsMongo())

        // insert updated variant and delete the existing one
        mongoTemplate.save(updatedVariant, VARIANTS_COLLECTION)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(orgVariant.getId())), VARIANTS_COLLECTION)

        // remediate Annotations
        remediateAnnotations(orgVariant.getId(), newId)
    }

    void remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, VariantDocument orgVariant, String newId,
                                                 String updatedChromosome) {
        Set<VariantSourceEntryMongo> updatedFiles = orgVariant.getVariantSources().stream()
                .peek(vse -> {
                    if (vse.getAttrs() != null) {
                        vse.getAttrs().append("CHR", updatedChromosome);
                    }
                })
                .collect(Collectors.toSet())
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), updatedFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", updatedFiles.stream()
                        .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

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

        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(orgVariant.getId())), VARIANTS_COLLECTION)

        remediateAnnotations(orgVariant.getId(), newId)
    }

    void remediateCaseMergeAllCommonSidFidHasOneFile(VariantDocument variantInDB,
                                                     VariantDocument orgVariant, Set<Pair> sidFidPairNotInDB,
                                                     String newId, String updatedChromosome) {
        Set<VariantSourceEntryMongo> candidateFiles = orgVariant.getVariantSources().stream()
                .filter(vse -> sidFidPairNotInDB.contains(new Pair(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())
        Set<VariantSourceEntryMongo> updatedFiles = candidateFiles.stream()
                .peek(vse -> {
                    if (vse.getAttrs() != null) {
                        vse.getAttrs().append("CHR", updatedChromosome);
                    }
                })
                .collect(Collectors.toSet())

        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), updatedFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", updatedFiles.stream()
                        .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

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

        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(orgVariant.getId())), VARIANTS_COLLECTION)

        remediateAnnotations(orgVariant.getId(), newId)
    }

    void remediateAnnotations(String orgVariantId, String newVariantId) {
        String escapedOrgVariantId = Pattern.quote(orgVariantId)
        String escapedNewVariantId = Pattern.quote(newVariantId)
        // Fix associated annotations - remove the existing one and insert updated one if not present
        Query annotationsCombinedRegexQuery = new Query(
                new Criteria().orOperator(
                        where("_id").regex("^" + escapedOrgVariantId + ".*"),
                        where("_id").regex("^" + escapedNewVariantId + ".*")
                )
        )
        try {
            List<Document> annotationsList = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                    .find(annotationsCombinedRegexQuery.getQueryObject())
                    .into(new ArrayList<>())
            Set<String> updatedAnnotationIdSet = annotationsList.stream()
                    .filter(doc -> doc.get("_id").toString().startsWith(newVariantId))
                    .map(doc -> doc.get("_id"))
                    .collect(Collectors.toSet())
            Set<Document> orgAnnotationsSet = annotationsList.stream()
                    .filter(doc -> doc.get("_id").toString().startsWith(orgVariantId))
                    .collect(Collectors.toSet())
            for (Document annotation : orgAnnotationsSet) {
                // if corresponding updated annotation is already present skip it else insert it
                String orgAnnotationId = annotation.get("_id")
                String updatedAnnotationId = orgAnnotationId.replace(orgVariantId, newVariantId)
                if (!updatedAnnotationIdSet.contains(updatedAnnotationId)) {
                    annotation.put("_id", updatedAnnotationId)
                    mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertOne(annotation)
                }
                // delete the original annotation
                mongoTemplate.remove(Query.query(Criteria.where("_id").is(orgAnnotationId)), ANNOTATIONS_COLLECTION)
            }
        } catch (BsonSerializationException ex) {
            logger.error("Exception occurred while trying to remediate annotation for variant: {}", orgVariantId)
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
}


class VariantStatsProcessor {
    private static final String GENOTYPE_COUNTS_MAP = "genotypeCountsMap"
    private static final String ALLELE_COUNTS_MAP = "alleleCountsMap"
    private static final String MISSING_GENOTYPE = "missingGenotype"
    private static final String MISSING_ALLELE = "missingAllele"
    private static final String DEFAULT_GENOTYPE = "def"
    private static final List<String> MISSING_GENOTYPE_ALLELE_REPRESENTATIONS = Arrays.asList(".", "-1")

    Set<VariantStatsMongo> process(String ref, String alt, Set<VariantSourceEntryMongo> variantInDBVariantSourceEntryMongo,
                                   Set<VariantSourceEntryMongo> orgVariantSourceEntryMongo, sidFidNumberOfSamplesMap) {
        Set<VariantStatsMongo> variantStatsSet = new HashSet<>()

        if (sidFidNumberOfSamplesMap.isEmpty()) {
            // No new stats can be calculated, no processing required
            return variantStatsSet
        }

        Set<VariantSourceEntryMongo> variantSourceAll = variantInDBVariantSourceEntryMongo
        variantSourceAll.addAll(orgVariantSourceEntryMongo)

        Set<String> sidFidSet = sidFidNumberOfSamplesMap.keySet()

        // get only the ones for which we can calculate the stats
        Set<VariantSourceEntryMongo> variantSourceEntrySet = variantSourceAll.stream()
                .filter(vse -> sidFidSet.contains(vse.getStudyId() + "_" + vse.getFileId()))
                .collect(Collectors.toSet())

        for (VariantSourceEntryMongo variantSourceEntry : variantSourceEntrySet) {
            String studyId = variantSourceEntry.getStudyId()
            String fileId = variantSourceEntry.getFileId()

            BasicDBObject sampleData = variantSourceEntry.getSampleData()
            if (sampleData == null || sampleData.isEmpty()) {
                continue
            }

            VariantStats variantStats = getVariantStats(ref, alt, variantSourceEntry.getAlternates(), sampleData, sidFidNumberOfSamplesMap.get(studyId + "_" + fileId))
            VariantStatsMongo variantStatsMongo = new VariantStatsMongo(studyId, fileId, "ALL", variantStats)

            variantStatsSet.add(variantStatsMongo)
        }

        return variantStatsSet
    }

    VariantStats getVariantStats(String variantRef, String variantAlt, String[] fileAlternates, BasicDBObject sampleData, int totalSamplesForFileId) {
        Map<String, Map<String, Integer>> countsMap = getGenotypeAndAllelesCounts(sampleData, totalSamplesForFileId)
        Map<String, Integer> genotypeCountsMap = countsMap.get(GENOTYPE_COUNTS_MAP)
        Map<String, Integer> alleleCountsMap = countsMap.get(ALLELE_COUNTS_MAP)

        // Calculate Genotype Stats
        int missingGenotypes = genotypeCountsMap.getOrDefault(MISSING_GENOTYPE, 0)
        genotypeCountsMap.remove(MISSING_GENOTYPE)
        Map<Genotype, Integer> genotypeCount = genotypeCountsMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> new Genotype(entry.getKey(), variantRef, variantAlt), entry -> entry.getValue()))
        // find the minor genotype i.e. second highest entry in terms of counts
        Optional<Map.Entry<String, Integer>> minorGenotypeEntry = genotypeCountsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .skip(1)
                .findFirst()
        String minorGenotype = ""
        float minorGenotypeFrequency = 0.0f
        if (minorGenotypeEntry.isPresent()) {
            minorGenotype = minorGenotypeEntry.get().getKey()
            int totalGenotypes = genotypeCountsMap.values().stream().reduce(0, Integer::sum)
            minorGenotypeFrequency = (float) minorGenotypeEntry.get().getValue() / totalGenotypes
        }


        // Calculate Allele Stats
        int missingAlleles = alleleCountsMap.getOrDefault(MISSING_ALLELE, 0)
        alleleCountsMap.remove(MISSING_ALLELE)
        // find the minor allele i.e. second highest entry in terms of counts
        Optional<Map.Entry<String, Integer>> minorAlleleEntry = alleleCountsMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .skip(1)
                .findFirst()
        String minorAllele = ""
        float minorAlleleFrequency = 0.0f
        if (minorAlleleEntry.isPresent()) {
            int minorAlleleEntryCount = minorAlleleEntry.get().getValue()
            int totalAlleles = alleleCountsMap.values().stream().reduce(0, Integer::sum)
            minorAlleleFrequency = (float) minorAlleleEntryCount / totalAlleles

            String minorAlleleKey = alleleCountsMap.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(minorAlleleEntryCount))
                    .sorted(Map.Entry.comparingByKey())
                    .findFirst()
                    .get()
                    .getKey()

            minorAllele = minorAlleleKey.equals("0") ? variantRef : minorAlleleKey.equals("1") ? variantAlt : fileAlternates[Integer.parseInt(minorAlleleKey) - 2]
        }

        VariantStats variantStats = new VariantStats()
        variantStats.setRefAllele(variantRef)
        variantStats.setAltAllele(variantAlt)
        variantStats.setMissingGenotypes(missingGenotypes)
        variantStats.setMgf(minorGenotypeFrequency)
        variantStats.setMgfGenotype(minorGenotype)
        variantStats.setGenotypesCount(genotypeCount)
        variantStats.setMissingAlleles(missingAlleles)
        variantStats.setMaf(minorAlleleFrequency)
        variantStats.setMafAllele(minorAllele)

        return variantStats
    }

    private Map<String, Map<String, Integer>> getGenotypeAndAllelesCounts(BasicDBObject sampleData, int totalSamplesForFileId) {
        Map<String, Map<String, Integer>> genotypeAndAllelesCountsMap = new HashMap<>()
        Map<String, Integer> genotypeCountsMap = new HashMap<>()
        Map<String, Integer> alleleCountsMap = new HashMap<>()

        String defaultGenotype = ""
        for (Map.Entry<String, Object> entry : sampleData.entrySet()) {
            String genotype = entry.getKey()
            if (genotype.equals(DEFAULT_GENOTYPE)) {
                defaultGenotype = entry.getValue().toString()
                continue
            }

            int noOfSamples = ((List<Integer>) entry.getValue()).size()
            String[] genotypeParts = genotype.split("\\||/")

            if (Arrays.stream(genotypeParts).anyMatch(gp -> MISSING_GENOTYPE_ALLELE_REPRESENTATIONS.contains(gp))) {
                genotypeCountsMap.put(MISSING_GENOTYPE, genotypeCountsMap.getOrDefault(MISSING_GENOTYPE, 0) + 1)
            } else {
                genotypeCountsMap.put(genotype, noOfSamples)
            }

            for (String genotypePart : genotypeParts) {
                if (MISSING_GENOTYPE_ALLELE_REPRESENTATIONS.contains(genotypePart)) {
                    alleleCountsMap.put(MISSING_ALLELE, alleleCountsMap.getOrDefault(MISSING_ALLELE, 0) + noOfSamples)
                } else {
                    alleleCountsMap.put(genotypePart, alleleCountsMap.getOrDefault(genotypePart, 0) + noOfSamples)
                }
            }
        }

        if (!defaultGenotype.isEmpty()) {
            int defaultGenotypeCount = totalSamplesForFileId - genotypeCountsMap.values().stream().reduce(0, Integer::sum)

            String[] genotypeParts = defaultGenotype.split("\\||/")
            if (Arrays.stream(genotypeParts).anyMatch(gp -> MISSING_GENOTYPE_ALLELE_REPRESENTATIONS.contains(gp))) {
                genotypeCountsMap.put(MISSING_GENOTYPE, genotypeCountsMap.getOrDefault(MISSING_GENOTYPE, 0) + 1)
            } else {
                genotypeCountsMap.put(defaultGenotype, defaultGenotypeCount)
            }

            for (String genotypePart : genotypeParts) {
                if (MISSING_GENOTYPE_ALLELE_REPRESENTATIONS.contains(genotypePart)) {
                    alleleCountsMap.put(MISSING_ALLELE, alleleCountsMap.getOrDefault(MISSING_ALLELE, 0) + defaultGenotypeCount)
                } else {
                    alleleCountsMap.put(genotypePart, alleleCountsMap.getOrDefault(genotypePart, 0) + defaultGenotypeCount)
                }
            }
        }

        genotypeAndAllelesCountsMap.put(GENOTYPE_COUNTS_MAP, genotypeCountsMap)
        genotypeAndAllelesCountsMap.put(ALLELE_COUNTS_MAP, alleleCountsMap)

        return genotypeAndAllelesCountsMap
    }

}
