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

import java.nio.file.Paths
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
                    logger.error("There was an error processing batch. " + e)
                }
                TOTAL_COUNTS += BATCH_COUNT
                logger.info("Total document processed till now: {}", TOTAL_COUNTS)
                annotationsBatch.clear()
                BATCH_COUNT = 0
            }
        }

        if (annotationsBatch.size() > 0) {
            try {
                processAnnotationsBatch(annotationsBatch)
            } catch (Exception e) {
                logger.error("There was an error processing batch. " + e)
            }
            TOTAL_COUNTS += BATCH_COUNT
            logger.info("Total document processed till now: {}", TOTAL_COUNTS)
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
        // get the variant ids for annotations for which we are trying to update
        List<String> variantIdList = annotationsWithNonInsdcChromosome.stream()
                .map(annotUpdate -> annotUpdate.getVariantId())
                .collect(Collectors.toList())
        // check if the variant is still present in the DB
        Query query = new Query(where("_id").in(variantIdList));
        query.fields().include("_id");
        Set<String> variantsIdsInDB = mongoTemplate.find(query, Document.class, VARIANTS_COLLECTION)
                .stream().map(variantDoc -> variantDoc.get("_id"))
                .collect(Collectors.toSet())
        // remove all those annotations for which variant id is still present in the db (variant not remediated yet), these will not be remediated
        List<AnnotationUpdateModel> annotationsToBeUpdatedWithInsdcChromosomes = annotationsWithNonInsdcChromosome.stream()
                .filter(annotUpdate -> !variantsIdsInDB.contains(annotUpdate.getVariantId()))
                .collect(Collectors.toList())

        if (!annotationsToBeUpdatedWithInsdcChromosomes.isEmpty()) {
            List<WriteModel<Document>> bulkUpdateOperations = new ArrayList<>()

            for (AnnotationUpdateModel annotationUpdateModel : annotationsToBeUpdatedWithInsdcChromosomes) {
                // update annotation id and chr to be INSDC
                Document updatedAnnotDocument = new Document(annotationUpdateModel.getOrginalAnnotationDocument())
                String newId = annotationUpdateModel.getAnnotationId().replace(annotationUpdateModel.getChromosome(), annotationUpdateModel.getInsdcChromosome())
                updatedAnnotDocument["_id"] = newId
                updatedAnnotDocument["chr"] = annotationUpdateModel.getInsdcChromosome()
                bulkUpdateOperations.add(new ReplaceOneModel<>(
                        Filters.eq("_id", newId),
                        updatedAnnotDocument,
                        new ReplaceOptions().upsert(true)
                ))
            }

            mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).bulkWrite(bulkUpdateOperations)

            // delete existing annotations with non insdc chromosomes
            Set<String> documentsToDeleteIdList = annotationsToBeUpdatedWithInsdcChromosomes.stream()
                    .map(annotUpdate -> annotUpdate.getAnnotationId()).collect(Collectors.toSet())
            mongoTemplate.remove(Query.query(Criteria.where("_id").in(documentsToDeleteIdList)), ANNOTATIONS_COLLECTION)
        }


        /* check annotations for mismatch of chr in the Id and chr field */

        // get all those annotations Ids which are going to be updated as part of the insdc chromosome update
        Set<String> alreadyUpdatedAnnotationIds = annotationsToBeUpdatedWithInsdcChromosomes.stream()
                .map(annotUpdate -> annotUpdate.getAnnotationId())
                .collect(Collectors.toSet())
        // find the annotations to be updated for chr mismatch (removing all updated for INSDC)
        List<AnnotationUpdateModel> annotationsWithMismatchChromosomeList = annotationUpdateModelList.stream()
                .filter(annotUpdate -> !annotUpdate.getChromosome().equals(annotUpdate.getChromosomeFromChrField()))
                .filter(annotUpdate -> !alreadyUpdatedAnnotationIds.contains(annotUpdate.getAnnotationId()))
                .collect(Collectors.toList())

        // update chromosome field with the chromosome from the id
        if (!annotationsWithMismatchChromosomeList.isEmpty()) {
            List<WriteModel<Document>> bulkUpdateOperations = new ArrayList<>()
            for (AnnotationUpdateModel annotationUpdateModel : annotationsWithMismatchChromosomeList) {
                bulkUpdateOperations.add(new UpdateOneModel<>(Filters.eq("_id", annotationUpdateModel.getAnnotationId()),
                        Updates.set("chr", annotationUpdateModel.getChromosome())))
            }
            mongoTemplate.getCollection(ANNOTATIONS_COLLECTION).bulkWrite(bulkUpdateOperations)
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
        if (id instanceof byte[]) {
            return id
        }
        if (id instanceof Binary) {
            return id.getData()
        }
        if (id instanceof ObjectId) {
            return id.toByteArray()
        }

        throw new IllegalArgumentException("Unsupported _id type: ${id?.getClass()}")
    }
}


class AnnotationUpdateModel {
    Document orginalAnnotationDocument
    String annotationId
    String chromosomeFromChrField
    String chromosome
    String variantId
    String insdcChromosome

    AnnotationUpdateModel(Document annotationDocument) {
        this.orginalAnnotationDocument = annotationDocument
        this.annotationId = annotationDocument.get("_id")
        this.chromosomeFromChrField = annotationDocument.get("chr")
        this.chromosome = this.getChromosomeFromAnnotationId(this.annotationId)
        this.variantId = this.getVariantIdFromAnnotationId(this.annotationId)
        this.insdcChromosome = null
    }

    private String getChromosomeFromAnnotationId(String annotationId) {
        return annotationId.substring(0, annotationId.indexOf("_"))
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

    void setInsdcChromosome(String insdcChromosome) {
        this.insdcChromosome = insdcChromosome
    }
}