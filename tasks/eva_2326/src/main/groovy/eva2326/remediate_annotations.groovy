package eva2326

import groovy.cli.picocli.CliBuilder
import org.bson.Document
import org.bson.BsonSerializationException
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
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument

import java.util.stream.Collectors

import static org.springframework.data.mongodb.core.query.Criteria.where

def cli = new CliBuilder()
cli.envPropertiesFile(args: 1, "Properties file with db details to use for update", required: true)
cli.dbName(args: 1, "Database name that needs to be updated", required: true)
cli.annotationRemediationInputFile(args: 1, "Path to CSV file with old_variant_id,new_variant_id,insdc_contig for annotation remediation", required: true)
cli.notRemediatedVariantsFilePath(args: 1, "Path to file where the variants which are not remediated will be stored", required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}

new SpringApplicationBuilder(RemediateAnnotationsApplication.class).properties([
        'spring.config.location'      : options.envPropertiesFile,
        'spring.data.mongodb.database': options.dbName])
        .run(options.annotationRemediationInputFile, options.notRemediatedVariantsFilePath, options.dbName)


@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class RemediateAnnotationsApplication implements CommandLineRunner {
    static Logger logger = LoggerFactory.getLogger(RemediateAnnotationsApplication.class)
    private static int BATCH_SIZE = 1000
    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    public static final Map<String, Set<String>> DB_VEP_CACHE_VERSION_SUFFIXES = [
            "eva_hsapiens_grch37": ["78_78"].toSet(),
            "eva_hsapiens_grch38": ["86_86"].toSet()
    ].asImmutable()

    @Autowired
    MongoTemplate mongoTemplate
    MappingMongoConverter converter
    String notRemediatedVariantsFilePath
    BufferedWriter notRemediatedWriter
    Set<String> vepCacheVersionSuffixes

    @Override
    void run(String... args) throws Exception {
        String annotationRemediationInputFile = args[0]
        this.notRemediatedVariantsFilePath = args[1]
        String dbName = args[2]

        this.vepCacheVersionSuffixes = DB_VEP_CACHE_VERSION_SUFFIXES.get(dbName)
        if (vepCacheVersionSuffixes == null || vepCacheVersionSuffixes.isEmpty()) {
            logger.error("No VEP/cache version suffixes configured for db: {}", dbName)
            System.exit(1)
        }


        // workaround to not save _class field in documents
        converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        File inputFile = new File(annotationRemediationInputFile)
        if (!inputFile.exists()) {
            logger.error("Annotation remediation input file does not exist: {}", annotationRemediationInputFile)
            System.exit(1)
        }

        notRemediatedWriter = new BufferedWriter(new FileWriter(notRemediatedVariantsFilePath, true))
        // Stream through the file line by line, accumulating into a batch.
        // Once the batch reaches BATCH_SIZE, process and clear it before reading further.
        try {
            List<String[]> batch = new ArrayList<>()
            int lineNumber = 0
            int skippedLines = 0
            int totalProcessed = 0

            inputFile.withReader { reader ->
                String line
                while ((line = reader.readLine()) != null) {
                    lineNumber++
                    String trimmed = line.trim()
                    if (!trimmed) {
                        continue
                    }

                    String[] parts = trimmed.split(",", -1)
                    if (parts.length < 3 || !parts[0].trim() || !parts[1].trim() || !parts[2].trim()) {
                        logger.warn("Skipping malformed line {} in input file: '{}'", lineNumber, trimmed)
                        skippedLines++
                        continue
                    }

                    batch.add([parts[0].trim(), parts[1].trim(), parts[2].trim()] as String[])

                    if (batch.size() >= RemediateAnnotationsApplication.BATCH_SIZE) {
                        processBatch(batch)
                        totalProcessed += batch.size()
                        logger.info("Total entries processed so far: {}", totalProcessed)
                        batch.clear()
                    }
                }
            }

            if (!batch.isEmpty()) {
                processBatch(batch)
                totalProcessed += batch.size()
                batch.clear()
            }

            logger.info("Annotation remediation complete. Total entries processed: {}, malformed lines skipped: {}",
                    totalProcessed, skippedLines)
        } finally {
            notRemediatedWriter.close()
        }

        System.exit(0)
    }

    void processBatch(List<String[]> batch) {
        // keyed by old variant id
        Map<String, String> orgIdNewIdMap = new LinkedHashMap<>()     // oldId -> newId
        Map<String, String> orgIdInsdcChrMap = new LinkedHashMap<>()  // oldId -> insdcContig

        for (String[] entry : batch) {
            orgIdNewIdMap.put(entry[0], entry[1])
            orgIdInsdcChrMap.put(entry[0], entry[2])
        }

        // Fetch all new variants from DB in one query for the whole batch
        Map<String, VariantDocument> variantsInDBMap = getVariantsByIds(new ArrayList<>(orgIdNewIdMap.values()))

        // Verify each entry: new variant must exist in DB with the expected INSDC contig.
        // Failures are logged and written to the not-remediated file; only verified entries proceed.
        Map<String, String> verifiedOrgIdNewIdMap = new LinkedHashMap<>()

        for (Map.Entry<String, String> entry : orgIdNewIdMap.entrySet()) {
            String oldVariantId = entry.getKey()
            String newVariantId = entry.getValue()

            VariantDocument variantInDB = variantsInDBMap.get(newVariantId)

            if (variantInDB == null) {
                logger.error("Variant check failed: new variant id {} not found in DB (old id: {}). " +
                        "Skipping annotation remediation for this variant.", newVariantId, oldVariantId)
                storeNotRemediatedVariant(oldVariantId, newVariantId, "New variant id not found in DB")
                continue
            }

            verifiedOrgIdNewIdMap.put(oldVariantId, newVariantId)
        }

        logger.info("Batch: {} entries passed variant check, {} skipped",
                verifiedOrgIdNewIdMap.size(), orgIdNewIdMap.size() - verifiedOrgIdNewIdMap.size())

        if (!verifiedOrgIdNewIdMap.isEmpty()) {
            logger.info("Remediate Annotations for Ids: " + verifiedOrgIdNewIdMap.keySet())
            remediateAnnotations(verifiedOrgIdNewIdMap, orgIdInsdcChrMap)
        }
    }

    Map<String, VariantDocument> getVariantsByIds(List<String> ids) {
        Query idQuery = new Query(where("_id").in(ids))
        List<VariantDocument> variants = mongoTemplate.find(idQuery, VariantDocument.class, VARIANTS_COLLECTION)
        return variants.stream().collect(Collectors.toMap(v -> v.getId(), v -> v))
    }

    void storeNotRemediatedVariant(String oldVariantId, String newVariantId, String reason) {
        try {
            notRemediatedWriter.write("${oldVariantId},${newVariantId},${reason}\n")
            notRemediatedWriter.flush()
        } catch (IOException e) {
            logger.error("Error writing to not-remediated variants file for old id {}: {}", oldVariantId, e.getMessage())
        }
    }

    void remediateAnnotations(Map<String, String> orgIdNewIdMap, Map<String, String> orgIdInsdcChrMap) {
        Set<String> idProcessed = new HashSet<>()

        List<String> allOldAnnotationIds = new ArrayList<>()
        List<String> allNewAnnotationIds = new ArrayList<>()
        Map<String, String> oldAnnotationIdToVariantId = new HashMap<>()

        for (Map.Entry<String, String> entry : orgIdNewIdMap.entrySet()) {
            String oldVariantId = entry.getKey()
            String newVariantId = entry.getValue()
            for (String vepVCacheV : vepCacheVersionSuffixes) {
                String oldAnnotationId = "${oldVariantId}_${vepVCacheV}"
                String newAnnotationId = "${newVariantId}_${vepVCacheV}"
                allOldAnnotationIds.add(oldAnnotationId)
                allNewAnnotationIds.add(newAnnotationId)
                oldAnnotationIdToVariantId.put(oldAnnotationId, oldVariantId)
            }
        }

        try {
            List<Document> oldAnnotationsList = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                    .find(new Query(Criteria.where("_id").in(allOldAnnotationIds)).getQueryObject())
                    .into(new ArrayList<>())
            List<Document> newAnnotationsList = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                    .find(new Query(Criteria.where("_id").in(allNewAnnotationIds)).getQueryObject())
                    .into(new ArrayList<>())
            Map<String, Set<Document>> oldVariantIdToDocuments = new HashMap<>()
            for (Document doc : oldAnnotationsList) {
                String variantId = oldAnnotationIdToVariantId.get(doc.getString("_id"))
                if (variantId != null) {
                    oldVariantIdToDocuments.computeIfAbsent(variantId, k -> new HashSet<>()).add(doc)
                } else {
                    logger.error("Could not map annotation id back to variant id: {}", doc.getString("_id"))
                }
            }

            Set<String> existingNewAnnotationIds = newAnnotationsList
                    .collect { it.getString("_id") }
                    .toSet()

            // Process each old->new id pair
            for (Map.Entry<String, String> entry : orgIdNewIdMap.entrySet()) {
                String orgVariantId = entry.getKey()
                String newVariantId = entry.getValue()

                Set<Document> orgAnnotationsSet = oldVariantIdToDocuments.getOrDefault(orgVariantId, Collections.emptySet())

                if (orgAnnotationsSet.isEmpty()) {
                    logger.info("No annotations found for old variant id {}, nothing to remediate", orgVariantId)
                    continue
                }

                List<Document> toInsert = new ArrayList<>()
                List<String> toDelete = new ArrayList<>()

                for (Document annotation : orgAnnotationsSet) {
                    try {
                        String orgAnnotationId = annotation.getString("_id")
                        String updatedAnnotationId = orgAnnotationId.replace(orgVariantId, newVariantId)

                        if (!existingNewAnnotationIds.contains(updatedAnnotationId)) {
                            if (!idProcessed.contains(updatedAnnotationId)) {
                                Document updated = new Document(annotation)
                                updated.put("_id", updatedAnnotationId)
                                updated.put("chr", orgIdInsdcChrMap.get(orgVariantId))
                                toInsert.add(updated)
                                idProcessed.add(updatedAnnotationId)
                            }
                        }

                        toDelete.add(orgAnnotationId)
                    } catch (Exception e) {
                        logger.error("Error processing annotation for original variant id {}: {}",
                                orgVariantId, e.getMessage(), e)
                    }
                }

                if (!toDelete.isEmpty()) {
                    mongoTemplate.remove(Query.query(Criteria.where("_id").in(toDelete)), ANNOTATIONS_COLLECTION)
                }
                if (!toInsert.isEmpty()) {
                    mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertMany(toInsert)
                }

                logger.info("Annotation remediation for old variant id {}: {} inserted, {} deleted",
                        orgVariantId, toInsert.size(), toDelete.size())
            }
        } catch (BsonSerializationException ex) {
            logger.error("Exception occurred while trying to update annotations: {}", ex.getMessage(), ex)
        }
    }
}