package eva4135

import com.mongodb.client.model.*
import groovy.cli.picocli.CliBuilder
import groovy.json.JsonSlurper
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.regex.Matcher
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
new SpringApplicationBuilder(UpdateAnnotationsApplication.class).properties([
        'spring.config.location'      : options.envPropertiesFile,
        'spring.data.mongodb.database': options.dbName])
        .run(options.workingDir, options.dbName, options.assemblyReportDir)


@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class])
class UpdateAnnotationsApplication implements CommandLineRunner {
    private static Logger logger = LoggerFactory.getLogger(UpdateAnnotationsApplication.class)
    private static int BATCH_SIZE = 1000
    private static long BATCH_COUNT = 0
    private static long TOTAL_COUNTS = 0
    public static final String VARIANTS_COLLECTION = "variants_2_0"
    public static final String ANNOTATIONS_COLLECTION = "annotations_2_0"
    private static final long LOG_INTERVAL = 1_000_000
    private static long nextLogThreshold = LOG_INTERVAL

    @Autowired
    MongoTemplate mongoTemplate

    ContigRenamingProcessor contigRenamer
    String annotationsWithIssuesFilePath

    @Override
    void run(String... args) throws Exception {
        String workingDir = args[0]
        String dbName = args[1]
        String assemblyReportDir = args[2]

        // create a dir to store variants that could not be processed due to various reasons
        String annotationsWithIssuesDirPath = Paths.get(workingDir, "annotations_with_issues").toString()
        File annotationsWithIssuesDir = new File(annotationsWithIssuesDirPath)
        if (!annotationsWithIssuesDir.exists()) {
            annotationsWithIssuesDir.mkdirs()
        }
        annotationsWithIssuesFilePath = Paths.get(annotationsWithIssuesDirPath, dbName + ".txt").toString()

        def (fastaPath, assemblyReportPath) = getFastaAndReportPaths(assemblyReportDir, dbName)
        // load assembly report for contig mapping
        contigRenamer = new ContigRenamingProcessor(assemblyReportPath, dbName)

        // Obtain a MongoCursor to iterate through documents
        def mongoCursor = mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                .find()
                .noCursorTimeout(true)
                .batchSize(BATCH_SIZE)
                .iterator()

        List<Document> annotationsBatch = new ArrayList<>()

        while (mongoCursor.hasNext()) {
            Document annotationDocument = mongoCursor.next()
            if (annotationDocument != null) {
                annotationsBatch.add(annotationDocument)
                BATCH_COUNT++
            }

            if (BATCH_COUNT >= BATCH_SIZE) {
                try {
                    processAnnotationsBatch(annotationsBatch)
                } catch (Exception e) {
                    logger.error("There was an error processing batch. ", e)
                }
                TOTAL_COUNTS += BATCH_COUNT
                if (TOTAL_COUNTS >= nextLogThreshold) {
                    logger.info("Processed {} annotation documents so far", TOTAL_COUNTS)
                    nextLogThreshold += LOG_INTERVAL
                }
                annotationsBatch.clear()
                BATCH_COUNT = 0
            }
        }

        if (annotationsBatch.size() > 0) {
            try {
                processAnnotationsBatch(annotationsBatch)
            } catch (Exception e) {
                logger.error("There was an error processing batch. ", e)
            }
            TOTAL_COUNTS += BATCH_COUNT
            logger.info("Finished processing total {} annotation documents", TOTAL_COUNTS)
            annotationsBatch.clear()
            BATCH_COUNT = 0
        }

        // Finished processing
        mongoCursor.close()
        System.exit(0)
    }

    void processAnnotationsBatch(List<Document> annotationsDocumentList) {
        List<AnnotationUpdateModel> annotationUpdateModelList = annotationsDocumentList.stream()
                .map(annotDoc -> new AnnotationUpdateModel(annotDoc))
                .collect(Collectors.toList())
        List<String> variantIdList = annotationUpdateModelList.stream()
                .map(annotUpdate -> annotUpdate.getVariantId())
                .collect(Collectors.toList())
        Query query = new Query(where("_id").in(variantIdList));
        query.fields().include("_id").include("chr").include("start").include("end")
        Map<String, Document> variantsIdsInDBMap = mongoTemplate.find(query, Document.class, VARIANTS_COLLECTION)
                .collectEntries { [(it.get("_id") as String): it] }

        // update insdc chromosome for all annotations
        for (AnnotationUpdateModel annotationUpdateModel : annotationUpdateModelList) {
            try {
                String orgChromosome = annotationUpdateModel.getChromosome()
                String insdcChromosome = contigRenamer.getInsdcAccession(orgChromosome)
                annotationUpdateModel.setInsdcChromosome(insdcChromosome)
            } catch (Exception e) {
                logger.error("Could not get INSDC accession for annotation {} with chromosome {}. Exception Message: {}",
                        annotationUpdateModel.getAnnotationId(), annotationUpdateModel.getChromosome(), e.getMessage())
            }
        }

        // store annotation ids for which we could not figure out INSDC chromosome
        logAnnotationIdsWithoutInsdcChromosome(annotationUpdateModelList)

        /*try and update annotations with non insdc chromosomes*/

        List<AnnotationUpdateModel> annotationsWithNonInsdcChromosome = annotationUpdateModelList.stream()
                .filter(annotUpdate -> annotUpdate.getInsdcChromosome() != null)
                .filter(annotUpdate -> !annotUpdate.getChromosome().equals(annotUpdate.getInsdcChromosome()))
                .collect(Collectors.toList())
        // remove all those annotations for which variant id is still present in the db (variant not remediated yet), these will not be remediated
        List<AnnotationUpdateModel> annotationsToBeUpdatedWithInsdcChromosomes = annotationsWithNonInsdcChromosome.stream()
                .filter(annotUpdate -> !variantsIdsInDBMap.containsKey(annotUpdate.getVariantId()))
                .collect(Collectors.toList())

        if (!annotationsToBeUpdatedWithInsdcChromosomes.isEmpty()) {
            logger.info("Documents to be updated because of non-insdc chromosome {}", annotationsToBeUpdatedWithInsdcChromosomes.size())

            for (AnnotationUpdateModel annotationUpdateModel : annotationsToBeUpdatedWithInsdcChromosomes) {
                Document updatedAnnotDocument = new Document(annotationUpdateModel.getOrginalAnnotationDocument())
                String newId = annotationUpdateModel.getAnnotationId().replaceFirst(
                        "^" + Pattern.quote(annotationUpdateModel.getChromosome()),
                        Matcher.quoteReplacement(annotationUpdateModel.getInsdcChromosome())
                )
                updatedAnnotDocument["_id"] = newId
                updatedAnnotDocument["chr"] = annotationUpdateModel.getInsdcChromosome()

                String oldId = annotationUpdateModel.getAnnotationId()

                try {
                    // Try insert first (target doesn't exist — happy path)
                    mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertOne(updatedAnnotDocument)
                    // Insert succeeded — safe to delete original
                    mongoTemplate.remove(Query.query(Criteria.where("_id").is(oldId)), ANNOTATIONS_COLLECTION)
                } catch (com.mongodb.MongoWriteException e) {
                    if (e.getCode() == 11000) {
                        // Duplicate key — target already exists with possibly stale chr/start/end
                        // Must delete it first (can't $set shard key fields), then re-insert
                        try {
                            mongoTemplate.remove(Query.query(Criteria.where("_id").is(newId)), ANNOTATIONS_COLLECTION)
                            mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertOne(updatedAnnotDocument)
                            // Both succeeded — safe to delete original
                            mongoTemplate.remove(Query.query(Criteria.where("_id").is(oldId)), ANNOTATIONS_COLLECTION)
                        } catch (Exception retryEx) {
                            logger.error("Failed to replace existing target annotation {}. Original id: {}. Exception: {}",
                                    newId, oldId, retryEx.getMessage())
                            // Original is kept intact — no data loss
                        }
                    } else {
                        logger.error("Failed to insert updated annotation document with new id {}. Original id: {}. Exception: {}",
                                newId, oldId, e.getMessage())
                    }
                } catch (Exception e) {
                    logger.error("Failed to insert updated annotation document with new id {}. Original id: {}. Exception: {}",
                            newId, oldId, e.getMessage())
                }
            }

        }


        /* check annotations for mismatch of chr, start and end in the Id and chr field */

        // get all those annotation Ids which has already been updated as part of the insdc chromosome update
        Set<String> alreadyUpdatedChrAnnotationIds = annotationsToBeUpdatedWithInsdcChromosomes.stream()
                .map(annotUpdate -> annotUpdate.getAnnotationId())
                .collect(Collectors.toSet())

        // update mismatch fields (in case of mismatch, update chromosome field with the chromosome from the annotation id, start and end field from the variant)
        List<WriteModel<Document>> bulkUpdateOperations = new ArrayList<>()
        List<WriteModel<Document>> bulkReplaceOperations = new ArrayList<>()
        for (AnnotationUpdateModel annotationUpdateModel : annotationUpdateModelList) {
            Document originalAnnotDoc = annotationUpdateModel.getOrginalAnnotationDocument()
            Document originalVariant = variantsIdsInDBMap.get(annotationUpdateModel.getVariantId())

            Document fieldsToUpdate = new Document()
            boolean shardKeyAffected = false

            // check for chromosome mismatch
            if (!annotationUpdateModel.getChromosome().equals(annotationUpdateModel.getChromosomeFromChrField())
                    && !alreadyUpdatedChrAnnotationIds.contains(annotationUpdateModel.getAnnotationId())) {
                fieldsToUpdate.put("chr", annotationUpdateModel.getChromosome())
                shardKeyAffected = true
            }

            if (originalVariant != null) {
                long orgVariantStart = AnnotationUpdateModel.getLongValue(originalVariant, "start")
                long orgVariantEnd = AnnotationUpdateModel.getLongValue(originalVariant, "end")
                // check for start mismatch
                if (!orgVariantStart.equals(annotationUpdateModel.getStart())) {
                    fieldsToUpdate.put("start", originalVariant.get("start"))
                    shardKeyAffected = true
                }
                // check for end mismatch
                if (!orgVariantEnd.equals(annotationUpdateModel.getEnd())) {
                    fieldsToUpdate.put("end", originalVariant.get("end"))
                }
            }

            if (!fieldsToUpdate.isEmpty()) {
                if (shardKeyAffected) {
                    // Build a full replacement document to avoid cross-shard transaction
                    Document replacementDoc = new Document(originalAnnotDoc)
                    fieldsToUpdate.each { k, v -> replacementDoc.put(k, v) }
                    bulkReplaceOperations.add(new ReplaceOneModel<>(
                            Filters.and(
                                    Filters.eq("_id", annotationUpdateModel.getAnnotationId()),
                                    Filters.eq("chr", originalAnnotDoc.get("chr")),
                                    Filters.eq("start", originalAnnotDoc.get("start"))
                            ),
                            replacementDoc
                    ))
                } else {
                    // Only non-shard-key fields (e.g. just end) — plain update is safe
                    Document update = new Document('$set', fieldsToUpdate)
                    bulkUpdateOperations.add(new UpdateOneModel<>(
                            Filters.and(
                                    Filters.eq("_id", annotationUpdateModel.getAnnotationId()),
                                    Filters.eq("chr", originalAnnotDoc.get("chr")),
                                    Filters.eq("start", originalAnnotDoc.get("start"))
                            ),
                            update
                    ))
                }
            }
        }

        if (!bulkReplaceOperations.isEmpty()) {
            logger.info("Number of documents to be replaced due to shard-key field mismatch (chr/start): {}", bulkReplaceOperations.size())
            // Cannot use replaceOne when shard key values change — MongoDB will attempt a
            // cross-shard transaction which fails on replica sets with arbiters.
            // Use delete + insert instead (same pattern as the INSDC chromosome section above).
            for (WriteModel<Document> op : bulkReplaceOperations) {
                ReplaceOneModel<Document> replaceOp = (ReplaceOneModel<Document>) op
                Document replacementDoc = replaceOp.getReplacement()
                String docId = replacementDoc.get("_id") as String

                try {
                    mongoTemplate.remove(Query.query(Criteria.where("_id").is(docId)), ANNOTATIONS_COLLECTION)
                    mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).insertOne(replacementDoc)
                } catch (com.mongodb.MongoWriteException e) {
                    if (e.getCode() == 11000) {
                        logger.error("Duplicate key on re-insert after delete for annotation id {}. Exception: {}",
                                docId, e.getMessage())
                    } else {
                        logger.error("Failed to replace annotation document with id {}. Exception: {}",
                                docId, e.getMessage())
                    }
                } catch (Exception e) {
                    logger.error("Failed to replace annotation document with id {}. Exception: {}",
                            docId, e.getMessage())
                }
            }
        }

        if (!bulkUpdateOperations.isEmpty()) {
            logger.info("Number of documents to be updated due to non-shard-key field mismatch (end): {}", bulkUpdateOperations.size())
            for (WriteModel<Document> op : bulkUpdateOperations) {
                mongoTemplate.getCollection(ANNOTATIONS_COLLECTION)
                        .bulkWrite([op], new BulkWriteOptions().ordered(false))
            }
        }
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

    void logAnnotationIdsWithoutInsdcChromosome(List<AnnotationUpdateModel> list) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(annotationsWithIssuesFilePath, true))) {
            list.stream()
                    .filter { it.getInsdcChromosome() == null }
                    .map { it.getAnnotationId() }
                    .forEach { id ->
                        byte[] bytes = extractBytes(id)
                        String base64 = Base64.encoder.encodeToString(bytes)
                        writer.write(base64)
                        writer.write(",")
                    }

        } catch (Exception e) {
            logger.error("Error storing annotation ids for missing INSDC chromosome", e)
        }
    }

    byte[] extractBytes(def id) {
        if (id == null) {
            return new byte[0]
        }
        if (id instanceof byte[]) {
            return id
        }
        if (id instanceof Binary) {
            return id.getData()
        }
        if (id instanceof ObjectId) {
            return id.toByteArray()
        }
        if (id instanceof String) {
            return id.getBytes(StandardCharsets.UTF_8)
        }

        return id.toString().getBytes(StandardCharsets.UTF_8)
    }
}


class AnnotationUpdateModel {
    Document orginalAnnotationDocument
    String annotationId
    String chromosomeFromChrField
    String chromosome
    String variantId
    String insdcChromosome
    long start
    long end

    AnnotationUpdateModel(Document annotationDocument) {
        this.orginalAnnotationDocument = annotationDocument
        this.annotationId = annotationDocument.get("_id")
        this.chromosomeFromChrField = annotationDocument.get("chr")
        this.chromosome = this.getChromosomeFromAnnotationId(this.annotationId)
        this.variantId = this.getVariantIdFromAnnotationId(this.annotationId)
        this.insdcChromosome = null
        this.start = getLongValue(annotationDocument, "start")
        this.end = getLongValue(annotationDocument, "end")
    }

    private String getChromosomeFromAnnotationId(String annotationId) {
        def m = annotationId =~ /^(.*?)(?=_[0-9]+_)/
        if (!m.find()) {
            throw new IllegalArgumentException("Cannot parse chromosome from annotation id: " + annotationId)
        }
        return m.group(1)
    }

    private String getVariantIdFromAnnotationId(String annotationId) {
        annotationId.substring(0, annotationId.lastIndexOf('_', annotationId.lastIndexOf('_') - 1))
    }

    Document getOrginalAnnotationDocument() {
        return orginalAnnotationDocument
    }

    String getAnnotationId() {
        return annotationId
    }

    String getChromosomeFromChrField() {
        return chromosomeFromChrField
    }

    String getChromosome() {
        return this.chromosome
    }

    String getVariantId() {
        return this.variantId
    }

    String getInsdcChromosome() {
        return insdcChromosome
    }

    long getStart() {
        return start
    }

    long getEnd() {
        return end
    }

    void setInsdcChromosome(String insdcChromosome) {
        this.insdcChromosome = insdcChromosome
    }

    static long getLongValue(Document doc, String key) {
        def value = doc.get(key)
        if (value instanceof Long) return value as Long
        if (value instanceof Integer) return (value as Integer).toLong()
        throw new IllegalArgumentException("Unexpected type for field '${key}': ${value?.class}")
    }
}