package eva3667

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import groovy.cli.picocli.CliBuilder
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

import java.nio.file.Paths
import java.util.regex.Pattern
import java.util.stream.Collectors

import static org.springframework.data.mongodb.core.query.Criteria.where

def cli = new CliBuilder()
cli.workingDir(args: 1, "Path to the working directory where processing files will be kept", required: true)
cli.envPropertiesFile(args: 1, "Properties file with db details to use for remediation", required: true)
cli.dbName(args: 1, "Database name that needs to be remediated", required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}


// Run the remediation application
new SpringApplicationBuilder(RemediationApplication.class).properties([
        'spring.config.location'      : options.envPropertiesFile,
        'spring.data.mongodb.database': options.dbName])
        .run(options.workingDir, options.dbName)


@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class RemediationApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(RemediationApplication.class)
    private static long counter = 0

    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String FILES_COLLECTION = "files_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    public static final String FILES_COLLECTION_STUDY_ID_KEY = "sid"
    public static final String FILES_COLLECTION_FILE_ID_KEY = "fid"

    public static final String REGEX_PATTERN = "(\\S+_){2}(?:(?=[^ACGTN_])\\S*|([ACGTN]*[^ACGTN_]+[ACGTN]*))_?(?:(?=[^ACGTN_])\\S*|([ACGTN]*[^ACGTN_]+[ACGTN]*))?"

    @Autowired
    MongoTemplate mongoTemplate

    private static Map<String, Integer> sidFidNumberOfSamplesMap = new HashMap<>()
    private static VariantStatsProcessor variantStatsProcessor = new VariantStatsProcessor()

    @Override
    void run(String... args) throws Exception {
        String workingDir = args[0]
        String dbName = args[1]

        // populate sidFidNumberOfSamplesMap
        populateFilesIdAndNumberOfSamplesMap()

        // workaround to not saving a field by the name _class (contains the java class name) in the document
        MappingMongoConverter converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(VARIANTS_COLLECTION)
        // Create a query to find all matching lowercase variants
        Query regexQuery = new Query(where("_id").regex(REGEX_PATTERN))
        // Obtain a MongoCursor to iterate through documents
        def mongoCursor = variantsColl.find(regexQuery.getQueryObject()).iterator()

        // Iterate through each variant one by one
        while (mongoCursor.hasNext()) {
            counter++
            logger.info("Processing Variant {}", counter)
            // read the variant as a VariantDocument
            VariantDocument lowercaseVariant = mongoTemplate.getConverter().read(VariantDocument.class, mongoCursor.next())
            logger.info("Processing Variant: {}", lowercaseVariant)

            // filter out false positives - double check if ref or alt really contains lowercase
            if (!hasLowerCaseRefOrAlt(lowercaseVariant)) {
                // False positive - variant captured by regex but does not have any lowercase character in ref or alt
                logger.info("Variant does not contain any lowercase ref or alt. variant_id: {}, variant_ref: {}, " +
                        "variant_alt: {}", lowercaseVariant.getId(), lowercaseVariant.getReference(),
                        lowercaseVariant.getAlternate())
                continue
            }

            logger.info("Variant contains lowercase ref or alt. variant_id: {}, variant_ref: {}, variant_alt: {}",
                    lowercaseVariant.getId(), lowercaseVariant.getReference(), lowercaseVariant.getAlternate())

            // create new id of variant by making ref and alt uppercase
            String newId = VariantDocument.buildVariantId(lowercaseVariant.getChromosome(), lowercaseVariant.getStart(),
                    lowercaseVariant.getReference().toUpperCase(), lowercaseVariant.getAlternate().toUpperCase())
            logger.info("New id of the variant : {}", newId)
            // check if new id is present in db and get the corresponding variant
            Query idQuery = new Query(where("_id").is(newId))
            VariantDocument variantInDB = mongoTemplate.findOne(idQuery, VariantDocument.class, VARIANTS_COLLECTION)

            // Check if there exists a variant in db that has the same id as newID
            if (variantInDB == null) {
                logger.warn("Variant with id {} not found in DB", newId)
                remediateCaseNoIdCollision(lowercaseVariant, newId)
                continue
            }

            logger.info("Found existing variant in DB with id: {} {}", newId, variantInDB)
            // variant with new db present, needs to check for merging
            Set<VariantSourceEntity> lowercaseVariantFileSet = lowercaseVariant.getVariantSources() != null ?
                    lowercaseVariant.getVariantSources() : new HashSet<>()
            Set<VariantSourceEntity> variantInDBFileSet = variantInDB.getVariantSources() != null ?
                    variantInDB.getVariantSources() : new HashSet<>()
            Set<Pair> lowercaseSidFidPairSet = lowercaseVariantFileSet.stream()
                    .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())
            Set<Pair> variantInDBSidFidPairSet = variantInDBFileSet.stream()
                    .map(vse -> new Pair(vse.getStudyId(), vse.getFileId()))
                    .collect(Collectors.toSet())

            // take the common pair of sid-fid between the lowercase variant and the variant in db
            Set<Pair> commonSidFidPairs = new HashSet<>(lowercaseSidFidPairSet)
            commonSidFidPairs.retainAll(variantInDBSidFidPairSet)

            if (commonSidFidPairs.isEmpty()) {
                logger.info("No common sid fid entries between lowercase variant and variant in DB")
                remediateCaseMergeAllSidFidAreDifferent(variantInDB, lowercaseVariant, newId)
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
                Set<Pair> sidFidPairNotInDB = new HashSet<>(lowercaseSidFidPairSet)
                sidFidPairNotInDB.removeAll(commonSidFidPairs)
                remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, lowercaseVariant, sidFidPairNotInDB, newId)
                continue
            }

            logger.info("can't merge as sid fid common pair has more than 1 entry in file")
            remediateCaseCantMerge(workingDir, sidFidPairsWithGTOneEntry, dbName, lowercaseVariant)
        }

        // Finished processing
        System.exit(0)
    }

    void remediateCaseNoIdCollision(VariantDocument lowercaseVariant, String newId) {
        logger.info("case no id collision - processing variant: {}", lowercaseVariant)

        Map<String, Set<String>> uppercaseHgvs = getUppercaseHgvs(lowercaseVariant)
        Set<VariantSourceEntryMongo> uppercaseFiles = getUppercaseFiles(lowercaseVariant.getVariantSources())
        Set<VariantStatsMongo> uppercaseStats = getUppercaseStats(lowercaseVariant.getVariantStatsMongo())

        VariantDocument upperCaseVariant = new VariantDocument(lowercaseVariant.getVariantType(), lowercaseVariant.getChromosome(),
                lowercaseVariant.getStart(), lowercaseVariant.getEnd(), lowercaseVariant.getLength(), lowercaseVariant.getReference(),
                lowercaseVariant.getAlternate(), uppercaseHgvs, lowercaseVariant.getIds(), uppercaseFiles)
        upperCaseVariant.setStats(uppercaseStats)

        // insert updated variant and delete the existing one with lowercase
        logger.info("case no id collision - insert new variant: {}", upperCaseVariant)
        mongoTemplate.save(upperCaseVariant, VARIANTS_COLLECTION)
        logger.info("case no id collision - delete lowercase variant: {}", lowercaseVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(lowercaseVariant.getId())), VARIANTS_COLLECTION)

        // remediate Annotations
        remediateAnnotations(lowercaseVariant.getId(), newId)
    }

    void remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, VariantDocument lowercaseVariant, String newId) {
        logger.info("case merge all sid fid are different - processing variant: {}", lowercaseVariant)

        Set<VariantSourceEntryMongo> upperCaseFiles = getUppercaseFiles(lowercaseVariant.getVariantSources())
        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), upperCaseFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", upperCaseFiles.stream()
                        .map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

        Map<String, Set<String>> uppercaseHgvs = getUppercaseHgvs(lowercaseVariant)
        if (!uppercaseHgvs.isEmpty()) {
            String uppercaseHgvsName = uppercaseHgvs.values().iterator().next()
            boolean hgvsNameAlreadyInDB = variantInDB.getHgvs().stream()
                    .map(hgvs -> hgvs.getName())
                    .anyMatch(name -> name.equals(uppercaseHgvsName))
            if (!hgvsNameAlreadyInDB) {
                HgvsMongo hgvsMongo = new HgvsMongo(uppercaseHgvs.keySet().iterator().next(), uppercaseHgvsName)
                updateOperations.add(Updates.push("hgvs", new Document("\$each", Collections.singletonList(
                        mongoTemplate.getConverter().convertToMongoType(hgvsMongo)))))
            }
        }

        logger.info("case merge all sid fid are different - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all sid fid are different - delete lowercase variant: {}", lowercaseVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(lowercaseVariant.getId())), VARIANTS_COLLECTION)

        remediateAnnotations(lowercaseVariant.getId(), newId)
    }

    void remediateCaseCantMerge(String workingDir, Set<Pair> sidFidPairsWithGtOneEntry, String dbName, VariantDocument lowercaseVariant) {
        logger.info("case can't merge variant - processing variant: {}", lowercaseVariant)

        String nmcDirPath = Paths.get(workingDir, "non_merged_candidates").toString()
        File nmcDir = new File(nmcDirPath)
        if (!nmcDir.exists()) {
            nmcDir.mkdirs()
        }
        String nmcFilePath = Paths.get(nmcDirPath, dbName + ".txt").toString()
        try (BufferedWriter nmcFile = new BufferedWriter(new FileWriter(nmcFilePath, true))) {
            for (Pair<String, String> p : sidFidPairsWithGtOneEntry) {
                nmcFile.write(p.getFirst() + "," + p.getSecond() + "," + lowercaseVariant.getId() + "\n")
            }
        } catch (IOException e) {
            logger.error("error writing case variant can't be merged in the file:  {}", lowercaseVariant)
        }
    }

    void remediateCaseMergeAllCommonSidFidHasOneFile(VariantDocument variantInDB,
                                                     VariantDocument lowercaseVariant, Set<Pair> sidFidPairNotInDB, String newId) {
        logger.info("case merge all common sid fid has one file - processing variant: {}", lowercaseVariant)

        Set<VariantSourceEntryMongo> candidateFiles = lowercaseVariant.getVariantSources().stream()
                .filter(vse -> sidFidPairNotInDB.contains(new Pair(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())
        Set<VariantSourceEntryMongo> upperCaseFiles = getUppercaseFiles(candidateFiles)

        Set<VariantStatsMongo> variantStats = variantStatsProcessor.process(variantInDB.getReference(),
                variantInDB.getAlternate(), variantInDB.getVariantSources(), upperCaseFiles, sidFidNumberOfSamplesMap)

        def updateOperations = [
                Updates.push("files", new Document("\$each", getUppercaseFiles(candidateFiles)
                        .stream().map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.set("st", variantStats.stream()
                        .map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList()))
        ]

        Map<String, Set<String>> uppercaseHgvs = getUppercaseHgvs(lowercaseVariant)
        if (!uppercaseHgvs.isEmpty()) {
            String uppercaseHgvsName = uppercaseHgvs.values().iterator().next()
            boolean hgvsNameAlreadyInDB = variantInDB.getHgvs().stream()
                    .map(hgvs -> hgvs.getName())
                    .anyMatch(name -> name.equals(uppercaseHgvsName))
            if (!hgvsNameAlreadyInDB) {
                HgvsMongo hgvsMongo = new HgvsMongo(uppercaseHgvs.keySet().iterator().next(), uppercaseHgvsName)
                updateOperations.add(Updates.push("hgvs", new Document("\$each", Collections.singletonList(
                        mongoTemplate.getConverter().convertToMongoType(hgvsMongo)))))
            }
        }

        logger.info("case merge all common sid fid has one file - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all common sid fid has one file - delete lowercase variant: {}", lowercaseVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(lowercaseVariant.getId())), VARIANTS_COLLECTION)

        remediateAnnotations(lowercaseVariant.getId(), newId)
    }

    void remediateAnnotations(String lowercaseVariantId, String newVariantId) {
        String escapedLowercaseVariantId = Pattern.quote(lowercaseVariantId)
        String escapedNewVariantId = Pattern.quote(newVariantId)
        // Fix associated annotations - remove the lowercase one and insert uppercase one if not present
        Query annotationsCombinedRegexQuery = new Query(
                new Criteria().orOperator(
                        where("_id").regex("^" + escapedLowercaseVariantId + ".*"),
                        where("_id").regex("^" + escapedNewVariantId + ".*")
                )
        )
        List<Document> annotationsList = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                .find(annotationsCombinedRegexQuery.getQueryObject())
                .into(new ArrayList<>())
        Set<String> uppercaseAnnotationIdSet = annotationsList.stream()
                .filter(doc -> doc.get("_id").toString().startsWith(newVariantId))
                .map(doc -> doc.get("_id"))
                .collect(Collectors.toSet())
        Set<Document> lowercaseAnnotationsSet = annotationsList.stream()
                .filter(doc -> doc.get("_id").toString().startsWith(lowercaseVariantId))
                .collect(Collectors.toSet())
        for (Document annotation : lowercaseAnnotationsSet) {
            // if corresponding uppercase annotation is already present skip it else insert it
            String lowercaseAnnotationId = annotation.get("_id")
            String uppercaseAnnotationId = lowercaseAnnotationId.replace(lowercaseVariantId, newVariantId)
            if (!uppercaseAnnotationIdSet.contains(uppercaseAnnotationId)) {
                annotation.put("_id", uppercaseAnnotationId)
                logger.info("insert uppercase annotation: {}", annotation)
                mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertOne(annotation)
            }
            // delete the lowercase annotation
            logger.info("delete lowercase annotation: {}", lowercaseAnnotationId)
            mongoTemplate.remove(Query.query(Criteria.where("_id").is(lowercaseAnnotationId)), ANNOTATIONS_COLLECTION)
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

        sidFidNumberOfSamplesMap = mongoTemplate.getCollection(RemediationApplication.FILES_COLLECTION)
                .aggregate(Arrays.asList(projectStage, groupStage, filterStage))
                .into(new ArrayList<>())
                .stream()
                .collect(Collectors.toMap({ doc -> doc.getString("_id") }, { doc -> doc.getInteger("totalNumOfSamples") }))
    }


    Map<Pair, Integer> getSidFidPairNumberOfDocumentsMap(Set<Pair> commonSidFidPairs) {
        List<Bson> filterConditions = new ArrayList<>()
        for (Pair sidFidPair : commonSidFidPairs) {
            filterConditions.add(Filters.and(Filters.eq(RemediationApplication.FILES_COLLECTION_STUDY_ID_KEY, sidFidPair.getFirst()),
                    Filters.eq(RemediationApplication.FILES_COLLECTION_FILE_ID_KEY, sidFidPair.getSecond())))
        }
        Bson filter = Filters.or(filterConditions)

        Map<Pair, Integer> sidFidPairCountMap = mongoTemplate.getCollection(FILES_COLLECTION).find(filter)
                .into(new ArrayList<>()).stream()
                .map(doc -> new Pair(doc.get(RemediationApplication.FILES_COLLECTION_STUDY_ID_KEY),
                        doc.get(RemediationApplication.FILES_COLLECTION_FILE_ID_KEY)))
                .collect(Collectors.toMap(pair -> pair, count -> 1, Integer::sum))

        return sidFidPairCountMap
    }

    Map<String, Set<String>> getUppercaseHgvs(VariantDocument variant) {
        Map<String, Set<String>> hgvs = new HashMap<>()
        if (variant.getVariantType().equals(Variant.VariantType.SNV)) {
            Set<String> hgvsCodes = new HashSet<>()
            hgvsCodes.add(variant.getChromosome() + ":g." + variant.getStart()
                    + variant.getReference().toUpperCase() + ">" + variant.getAlternate().toUpperCase())
            hgvs.put("genomic", hgvsCodes)
        }

        return hgvs
    }

    Set<VariantStatsMongo> getUppercaseStats(Set<VariantStatsMongo> variantStatsSet) {
        Set<VariantStatsMongo> variantStatsMongoSet = new HashSet<>()
        for (VariantStatsMongo stats : variantStatsSet) {
            variantStatsMongoSet.add(new VariantStatsMongo(stats.getStudyId(), stats.getFileId(), stats.getCohortId(),
                    stats.getMaf(), stats.getMgf(), stats.getMafAllele().toUpperCase(), stats.getMgfGenotype(),
                    stats.getMissingAlleles(), stats.getMissingGenotypes(), stats.getNumGt()))
        }

        return variantStatsMongoSet
    }

    Set<VariantSourceEntryMongo> getUppercaseFiles(Set<VariantSourceEntryMongo> variantSourceSet) {
        Set<VariantSourceEntryMongo> variantSourceEntryMongoSet = new HashSet<>()
        for (VariantSourceEntryMongo varSource : variantSourceSet) {
            variantSourceEntryMongoSet.add(new VariantSourceEntryMongo(varSource.getFileId(), varSource.getStudyId(),
                    varSource.getAlternates(), (BasicDBObject) varSource.getAttrs(), varSource.getFormat(), (BasicDBObject) varSource.getSampleData()))
        }

        return variantSourceEntryMongoSet
    }

    boolean hasLowerCaseRefOrAlt(VariantDocument variantDocument) {
        for (char c : variantDocument.getReference().toCharArray()) {
            if (Character.isLowerCase(c)) {
                return true
            }
        }
        for (char c : variantDocument.getAlternate().toCharArray()) {
            if (Character.isLowerCase(c)) {
                return true
            }
        }
        return false
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
                                   Set<VariantSourceEntryMongo> lowercaseVariantSourceEntryMongo, sidFidNumberOfSamplesMap) {
        Set<VariantStatsMongo> variantStatsSet = new HashSet<>()

        if (sidFidNumberOfSamplesMap.isEmpty()) {
            // No new stats can be calculated, no processing required
            return variantStatsSet
        }

        Set<VariantSourceEntryMongo> variantSourceAll = variantInDBVariantSourceEntryMongo
        variantSourceAll.addAll(lowercaseVariantSourceEntryMongo)

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
