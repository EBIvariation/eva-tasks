package eva3667

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates
import groovy.cli.picocli.CliBuilder
import org.bson.Document
import org.bson.conversions.Bson
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
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


@SpringBootApplication
class RemediationApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(RemediationApplication.class)

    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String FILES_COLLECTION = "files_2_0"
    public static final String FILES_COLLECTION_STUDY_ID_KEY = "sid"
    public static final String FILES_COLLECTION_FILE_ID_KEY = "fid"

    public static final String REGEX_PATTERN = '(?:\\S+_){2}([ACGTN]*[^ACGTN_]+[ACGTN]*)?_([ACGTN]*[^ACGTN_]+[ACGTN]*)?(?=\\s*$)'

    @Autowired
    MongoTemplate mongoTemplate

    @Override
    void run(String... args) throws Exception {
        String workingDir = args[0]
        String dbName = args[1]

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
            // read the variant as a VariantDocument
            VariantDocument lowercaseVariant = mongoTemplate.getConverter().read(VariantDocument.class, mongoCursor.next())
            logger.info("Processing Variant: {}", lowercaseVariant)

            // filter out false positives - double check if ref or alt really contains lowercase
            if (hasLowerCaseRefOrAlt(lowercaseVariant)) {
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
                if (variantInDB != null) {
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
                    } else {
                        // check if there is any pair of sid and fid from common pairs, for which there are more than one entry in files collection
                        Map<Pair, Integer> result = getSidFidPairNumberOfDocumentsMap(commonSidFidPairs)
                        Set<Pair> sidFidPairsWithGTOneEntry = result.entrySet().stream()
                                .filter(entry -> entry.getValue() > 1)
                                .map(entry -> entry.getKey())
                                .collect(Collectors.toSet())
                        if (sidFidPairsWithGTOneEntry.isEmpty()) {
                            Set<Pair> sidFidPairNotInDB = new HashSet<>(lowercaseSidFidPairSet)
                            sidFidPairNotInDB.removeAll(commonSidFidPairs)
                            remediateCaseMergeAllCommonSidFidHasOneFile(variantInDB, lowercaseVariant, sidFidPairNotInDB, newId)
                        } else {
                            remediateCaseCantMerge(workingDir, sidFidPairsWithGTOneEntry, dbName, lowercaseVariant)
                        }
                    }
                } else {
                    logger.warn("Variant with id {} not found in DB", newId)
                    remediateCaseNoIdCollision(lowercaseVariant, newId)
                }
            } else {
                // False positive - variant captured by regex but does not have any lowercase character in ref or alt
                logger.info("Variant does not contain any lowercase ref or alt. variant_id: {}, variant_ref: {}, " +
                        "variant_alt: {}", lowercaseVariant.getId(), lowercaseVariant.getReference(),
                        lowercaseVariant.getAlternate())
            }
        }

        // Finished processing
        return
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
    }

    void remediateCaseMergeAllSidFidAreDifferent(VariantDocument variantInDB, VariantDocument lowercaseVariant, String newId) {
        logger.info("case merge all sid fid are different - processing variant: {}", lowercaseVariant)

        def updateOperations = [
                Updates.push("files", new Document("\$each", getUppercaseFiles(lowercaseVariant.getVariantSources())
                        .stream().map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.push("st", new Document("\$each", getUppercaseStats(lowercaseVariant.getVariantStatsMongo())
                        .stream().map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList())))
        ]

        Map<String, Set<String>> uppercaseHgvs = getUppercaseHgvs(lowercaseVariant)
        String uppercaseHgvsName = uppercaseHgvs.values().iterator().next()
        boolean hgvsNameAlreadyInDB = variantInDB.getHgvs().stream()
                .map(hgvs -> hgvs.getName())
                .anyMatch(name -> name.equals(uppercaseHgvsName))
        if (!hgvsNameAlreadyInDB) {
            HgvsMongo hgvsMongo = new HgvsMongo(uppercaseHgvs.keySet().iterator().next(), uppercaseHgvsName)
            updateOperations.add(Updates.push("hgvs", new Document("\$each", Collections.singletonList(
                    mongoTemplate.getConverter().convertToMongoType(hgvsMongo)))))
        }

        logger.info("case merge all sid fid are different - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all sid fid are different - delete lowercase variant: {}", lowercaseVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(lowercaseVariant.getId())), VARIANTS_COLLECTION)
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
                nmcFile.write(p.getFirst() + "," + p.getSecond() + "\n")
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
        Set<VariantStatsMongo> candidateStats = lowercaseVariant.getVariantStatsMongo().stream()
                .filter(vse -> sidFidPairNotInDB.contains(new Pair(vse.getStudyId(), vse.getFileId())))
                .collect(Collectors.toSet())

        def updateOperations = [
                Updates.push("files", new Document("\$each", getUppercaseFiles(candidateFiles)
                        .stream().map(file -> mongoTemplate.getConverter().convertToMongoType(file))
                        .collect(Collectors.toList()))),
                Updates.push("st", new Document("\$each", getUppercaseStats(candidateStats)
                        .stream().map(stat -> mongoTemplate.getConverter().convertToMongoType(stat))
                        .collect(Collectors.toList())))
        ]

        Map<String, Set<String>> uppercaseHgvs = getUppercaseHgvs(lowercaseVariant)
        String uppercaseHgvsName = uppercaseHgvs.values().iterator().next()
        boolean hgvsNameAlreadyInDB = variantInDB.getHgvs().stream()
                .map(hgvs -> hgvs.getName())
                .anyMatch(name -> name.equals(uppercaseHgvsName))
        if (!hgvsNameAlreadyInDB) {
            HgvsMongo hgvsMongo = new HgvsMongo(uppercaseHgvs.keySet().iterator().next(), uppercaseHgvsName)
            updateOperations.add(Updates.push("hgvs", new Document("\$each", Collections.singletonList(
                    mongoTemplate.getConverter().convertToMongoType(hgvsMongo)))))
        }

        logger.info("case merge all common sid fid has one file - updates: {}", updateOperations)
        mongoTemplate.getCollection(VARIANTS_COLLECTION).updateOne(Filters.eq("_id", newId),
                Updates.combine(updateOperations))
        logger.info("case merge all common sid fid has one file - delete lowercase variant: {}", lowercaseVariant)
        mongoTemplate.remove(Query.query(Criteria.where("_id").is(lowercaseVariant.getId())), VARIANTS_COLLECTION)
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