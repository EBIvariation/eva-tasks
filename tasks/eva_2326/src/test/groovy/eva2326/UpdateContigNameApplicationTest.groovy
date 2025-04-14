package eva2326

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import org.bson.Document
import org.junit.jupiter.api.Test
import org.opencb.commons.utils.CryptoUtils
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.PropertiesPropertySource
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper
import org.springframework.data.mongodb.core.convert.MappingMongoConverter
import uk.ac.ebi.eva.commons.models.data.Variant
import uk.ac.ebi.eva.commons.models.mongo.entity.VariantDocument
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo

import java.nio.file.Paths
import java.util.stream.Collectors

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class UpdateContigNameApplicationTest {

    def setUpEnvAndRunApplicationWithQC(String testDBName, List<Document> filesData, List<Document> variantsData,
                                        List<Document> annotationsData, def qcMethod) {
        String resourceDir = "src/test/resources"
        String testPropertiesFile = resourceDir + "/application-test.properties"
        String workingDir = resourceDir + "/test_run"
        String assemblyReportDir = resourceDir
        File variantsWithIssuesFile = new File(Paths.get(workingDir, "/Variants_With_Issues/", testDBName + ".txt").toString())

        // Removing existing data and setup DB with test Data
        AnnotationConfigApplicationContext context = getApplicationContext(testPropertiesFile, testDBName)
        MongoTemplate mongoTemplate = context.getBean(MongoTemplate.class)
        MappingMongoConverter converter = mongoTemplate.getConverter()
        converter.setTypeMapper(new DefaultMongoTypeMapper(null))

        mongoTemplate.getCollection(UpdateContigApplication.FILES_COLLECTION).drop()
        mongoTemplate.getCollection(UpdateContigApplication.VARIANTS_COLLECTION).drop()
        mongoTemplate.getCollection(UpdateContigApplication.ANNOTATIONS_COLLECTION).drop()
        if (variantsWithIssuesFile.exists()) {
            variantsWithIssuesFile.delete()
        }
        if (filesData != null && !filesData.isEmpty()) {
            mongoTemplate.getCollection(UpdateContigApplication.FILES_COLLECTION).insertMany(filesData)
        }
        if (variantsData != null && !variantsData.isEmpty()) {
            mongoTemplate.getCollection(UpdateContigApplication.VARIANTS_COLLECTION).insertMany(variantsData)
        }
        if (annotationsData != null && !annotationsData.isEmpty()) {
            mongoTemplate.getCollection(UpdateContigApplication.ANNOTATIONS_COLLECTION).insertMany(annotationsData)
        }


        // Run Update
        UpdateContigApplication updateContigApplication = context.getBean(UpdateContigApplication.class)
        updateContigApplication.run(new String[]{workingDir, testDBName, assemblyReportDir})

        // Run QC
        qcMethod.call(mongoTemplate, workingDir, testDBName)

        context.close()
    }

    private AnnotationConfigApplicationContext getApplicationContext(String testPropertiesFile, String testDBName) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()
        def appProps = new Properties()
        appProps.load(new FileInputStream(new File(testPropertiesFile)))
        Map<String, Object> otherProperties = new HashMap<>()
        otherProperties.put("parameters.path", testPropertiesFile)
        otherProperties.put("spring.data.mongodb.database", testDBName)
        context.getEnvironment().getPropertySources().addLast(new PropertiesPropertySource("main", appProps))
        if (Objects.nonNull(otherProperties)) {
            context.getEnvironment().getPropertySources().addLast(new MapPropertySource("other", otherProperties))
        }
        context.register(UpdateContigApplication.class)
        context.refresh()

        return context
    }

    @Test
    void testUpdateContigNameWithDifferentCases() {
        String testDBName = "eva_fcatus_80"
        List<Document> filesData = getFilesDataForDifferentTestCases()
        List<Document> variantsData = getVariantsDataForDifferentTestCases()
        List<Document> annotationsData = getAnnotationsDataForDifferentTestCases()
        setUpEnvAndRunApplicationWithQC(testDBName, filesData, variantsData, annotationsData, this.&qcTestWithDifferentCases)
    }

    @Test
    void testUpdateContigNameHgvsNotPresent() {
        String testDBName = "eva_fcatus_80"
        List<Document> variantsData = getVariantsDataForHgvsNotPresent()
        setUpEnvAndRunApplicationWithQC(testDBName, new ArrayList<>(), variantsData, new ArrayList<>(), this.&qcTestNoHgvsPresent)
    }

    @Test
    void testUpdateContigNameCalculateStats() {
        String testDBName = "eva_fcatus_80"
        List<Document> filesData = getFilesDataForStatsCalculation()
        List<Document> variantsData = getVariantsDataForStatsCalculation()
        setUpEnvAndRunApplicationWithQC(testDBName, filesData, variantsData, new ArrayList<>(), this.&qcTestWithStatsCalculation)
    }


    List<Document> getFilesDataForDifferentTestCases() {
        return Arrays.asList(
                getFileDocument("sid21", "fid21", "file_name_21.vcf.gz")
                        .append("samp", new Document("samp211", 0).append("samp212", 1).append("samp213", 2)),
                getFileDocument("sid211", "fid211", "file_name_211.vcf.gz")
                        .append("samp", new Document("samp2111", 0).append("samp2112", 1).append("samp2113", 2)),
                getFileDocument("sid22", "fid22", "file_name_22.vcf.gz")
                        .append("samp", new Document("samp221", 0).append("samp222", 1).append("samp223", 2)),
                getFileDocument("sid222", "fid222", "file_name_222.vcf.gz")
                        .append("samp", new Document("samp2221", 0).append("samp2222", 1).append("samp2223", 2)),
                // case id collision - fid has more than one file
                getFileDocument("sid31", "fid31", "file_name_31.vcf.gz")
                        .append("samp", new Document("samp311", 0).append("samp312", 1).append("samp313", 2)),
                getFileDocument("sid31", "fid31", "file_name_31_1.vcf.gz")
                        .append("samp", new Document("samp311", 0).append("samp312", 1).append("samp313", 2)),
                //case id collision - fid has just one file
                getFileDocument("sid41", "fid41", "file_name_41.vcf.gz")
                        .append("samp", new Document("samp411", 0).append("samp412", 1).append("samp413", 2)),
                getFileDocument("sid411", "fid411", "file_name_411.vcf.gz")
                        .append("samp", new Document("samp4111", 0).append("samp4112", 1).append("samp4113", 2)),
                getFileDocument("sid42", "fid42", "file_name_42.vcf.gz")
                        .append("samp", new Document("samp421", 0).append("samp422", 1).append("samp423", 2)),
                getFileDocument("sid422", "fid422", "file_name_422.vcf.gz")
                        .append("samp", new Document("samp4221", 0).append("samp4222", 1).append("samp4223", 2))
        )
    }

    List<Document> getAnnotationsDataForDifferentTestCases() {
        return Arrays.asList(
                // new annotation id not present
                new Document("_id", "A1_11111111_A_G_82_82").append("cachev", 82).append("vepv", 82),
                // new annotation id present - delete existing and insert new
                new Document("_id", "A1_11111111_A_G_83_83").append("cachev", 83).append("vepv", 83),
                new Document("_id", "CM001378.3_11111111_A_G_83_83").append("cachev", 83).append("vepv", 83),
        )
    }

    List<Document> getVariantsDataForDifferentTestCases() {
        List<Document> variantDocumentList = new ArrayList<>()

        // case no id collision after update
        List<Document> hgvs11 = Arrays.asList(new Document("type", "genomic").append("name", "A1:g.11111111A>G"))
        List<Document> files11 = Arrays.asList(new Document("sid", "sid11").append("fid", "fid11"))
        List<Document> stats11 = Arrays.asList(getVariantStats("sid11", "fid11", 0.11, 0.11, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "A1", "A", "G",
                11111111, 11111111, 1, hgvs11, files11, stats11))
        // case no id collision after update - stats mafAl null
        List<Document> hgvs51 = Arrays.asList(new Document("type", "genomic").append("name", "B2:g.55555555A>G"))
        List<Document> files51 = Arrays.asList(new Document("sid", "sid51").append("fid", "fid51"))
        List<Document> stats51 = Arrays.asList(getVariantStats("sid51", "fid51", 0.51, 0.51, null, "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "B2", "A", "G",
                55555555, 55555555, 1, hgvs51, files51, stats51))

        // case id collision - all sid and fid are different
        // variant with non Insdc chromosome
        List<Document> hgvs21 = Arrays.asList(new Document("type", "genomic").append("name", "A2:g.22222222A>G"))
        List<Document> files21 = Arrays.asList(new Document("sid", "sid21").append("fid", "fid21").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                new Document("sid", "sid211").append("fid", "fid211").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        List<Document> stats21 = Arrays.asList(getVariantStats("sid21", "fid21", 0.21, 0.21, "A", "0/0"),
                getVariantStats("sid211", "fid211", 0.211, 0.211, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "A2", "A", "G",
                22222222, 22222222, 1, hgvs21, files21, stats21))

        // variant with insdc chromosome
        List<Document> hgvs22 = Arrays.asList(new Document("type", "genomic").append("name", "CM001379.3:g.22222222A>G"))
        List<Document> files22 = Arrays.asList(new Document("sid", "sid22").append("fid", "fid22").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                new Document("sid", "sid222").append("fid", "fid222").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        List<Document> stats22 = Arrays.asList(getVariantStats("sid22", "fid22", 0.22, 0.22, "A", "0/0"),
                getVariantStats("sid222", "fid222", 0.222, 0.222, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001379.3", "A", "G",
                22222222, 22222222, 1, hgvs22, files22, stats22))


        // case id collision - fid has more than one file
        // variant with insdc chromosome
        List<Document> hgvs31 = Arrays.asList(new Document("type", "genomic").append("name", "CM001380.3:g.33333333A>G"))
        List<Document> files31 = Arrays.asList(new Document("sid", "sid31").append("fid", "fid31"),
                new Document("sid", "sid311").append("fid", "fid311"))
        List<Document> stats31 = Arrays.asList(getVariantStats("sid31", "fid31", 0.31, 0.31, "A", "0/0"),
                getVariantStats("sid311", "fid311", 0.311, 0.311, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001380.3", "A", "G",
                33333333, 33333333, 1, hgvs31, files31, stats31))

        // variant with non insdc chromosome
        List<Document> hgvs32 = Arrays.asList(new Document("type", "genomic").append("name", "A3:g.33333333A>G"))
        List<Document> files32 = Arrays.asList(new Document("sid", "sid32").append("fid", "fid32"),
                new Document("sid", "sid322").append("fid", "fid322"),
                new Document("sid", "sid31").append("fid", "fid31"))
        List<Document> stats32 = Arrays.asList(getVariantStats("sid32", "fid32", 0.32, 0.32, "A", "0/0"),
                getVariantStats("sid322", "fid322", 0.322, 0.322, "A", "0/0"),
                getVariantStats("sid31", "fid31", 0.31, 0.31, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "A3", "A", "G",
                33333333, 33333333, 1, hgvs32, files32, stats32))


        // case id collision - common fid has just one file
        // variant with insdc chromosome
        List<Document> hgvs41 = Arrays.asList(new Document("type", "genomic").append("name", "CM001381.3:g.44444444A>G"))
        List<Document> files41 = Arrays.asList(new Document("sid", "sid41").append("fid", "fid41").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                new Document("sid", "sid411").append("fid", "fid411").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        List<Document> stats41 = Arrays.asList(getVariantStats("sid41", "fid41", 0.41, 0.41, "A", "0/0"),
                getVariantStats("sid411", "fid411", 0.411, 0.411, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001381.3", "A", "G",
                44444444, 44444444, 1, hgvs41, files41, stats41))

        // variant with non insdc chromosome
        List<Document> hgvs42 = Arrays.asList(new Document("type", "genomic").append("name", "B1:g.44444444A>G"))
        List<Document> files42 = Arrays.asList(new Document("sid", "sid42").append("fid", "fid42").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                new Document("sid", "sid422").append("fid", "fid422").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                new Document("sid", "sid411").append("fid", "fid411").append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        List<Document> stats42 = Arrays.asList(getVariantStats("sid42", "fid42", 0.42, 0.42, "A", "0/0"),
                getVariantStats("sid422", "fid422", 0.422, 0.422, "A", "0/0"),
                getVariantStats("sid411", "fid411", 0.411, 0.411, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "B1", "A", "G",
                44444444, 44444444, 1, hgvs42, files42, stats42))


        return variantDocumentList
    }

    void qcTestWithDifferentCases(MongoTemplate mongoTemplate, String workingDir, String dbName) {
        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(UpdateContigApplication.VARIANTS_COLLECTION)
        MongoCollection<Document> annotationsColl = mongoTemplate.getCollection(UpdateContigApplication.ANNOTATIONS_COLLECTION)
        // case_no_id_collision

        // assert non insdc variant deleted
        List<Document> nonInsdcVariantList = variantsColl.find(Filters.eq("_id", "A1_11111111_A_G")).into([])
        assertEquals(0, nonInsdcVariantList.size())
        // assert insdc variant inserted
        List<Document> insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001378.3_11111111_A_G")).into([])
        assertEquals(1, insdcVariantList.size())
        // assert all things updated to insdc in the updated variant
        Document insdcVariant = insdcVariantList.get(0)
        assertEquals('CM001378.3', insdcVariant.get("chr"))
        assertEquals('CM001378.3:g.11111111A>G', insdcVariant.get("hgvs")[0]["name"])

        // assert non insdc variant deleted
        nonInsdcVariantList = variantsColl.find(Filters.eq("_id", "B2_55555555_A_G")).into([])
        assertEquals(0, nonInsdcVariantList.size())
        // assert insdc variant inserted
        insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001382.3_55555555_A_G")).into([])
        assertEquals(1, insdcVariantList.size())
        // assert all things updated to insdc in the updated variant
        insdcVariant = insdcVariantList.get(0)
        assertEquals('CM001382.3', insdcVariant.get("chr"))
        assertEquals('CM001382.3:g.55555555A>G', insdcVariant.get("hgvs")[0]["name"])

        // assert annotation updated
        List<Document> nonInsdcAnnot1 = annotationsColl.find(Filters.eq("_id", "A1_11111111_A_G_82_82")).into([])
        List<Document> nonInsdcAnnot2 = annotationsColl.find(Filters.eq("_id", "A1_11111111_A_G_83_83")).into([])
        assertEquals(0, nonInsdcAnnot1.size())
        assertEquals(0, nonInsdcAnnot2.size())
        List<Document> insdcAnnot1 = annotationsColl.find(Filters.eq("_id", "CM001378.3_11111111_A_G_82_82")).into([])
        List<Document> insdcAnnot2 = annotationsColl.find(Filters.eq("_id", "CM001378.3_11111111_A_G_83_83")).into([])
        assertEquals(1, insdcAnnot1.size())
        assertEquals(1, insdcAnnot2.size())


        // case_id_collision_all_sid_fid_diff

        //  assert non insdc variant deleted
        nonInsdcVariantList = variantsColl.find(Filters.eq("_id", "A2_22222222_A_G")).into([])
        assertEquals(0, nonInsdcVariantList.size())
        // assert indsc variant inserted
        insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001379.3_22222222_A_G")).into([])
        assertEquals(1, insdcVariantList.size())

        // assert all things updated to insdc in the updated variant
        insdcVariant = insdcVariantList.get(0)
        assertEquals('CM001379.3', insdcVariant.get("chr"))
        assertEquals('CM001379.3:g.22222222A>G', insdcVariant.get("hgvs")[0]["name"])
        assertEquals(Arrays.asList("sid21", "sid211", "sid22", "sid222"),
                ((List<Document>) insdcVariant.get('files')).stream()
                        .map(doc -> doc.get("sid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("fid21", "fid211", "fid22", "fid222"),
                ((List<Document>) insdcVariant.get('files')).stream()
                        .map(doc -> doc.get("fid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("sid21", "sid211", "sid22", "sid222"),
                ((List<Document>) insdcVariant.get('st')).stream()
                        .map(doc -> doc.get("sid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("fid21", "fid211", "fid22", "fid222"),
                ((List<Document>) insdcVariant.get('st')).stream()
                        .map(doc -> doc.get("fid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("G", "G", "G", "G"),
                ((List<Document>) insdcVariant.get('st')).stream()
                        .map(doc -> doc.get("mafAl"))
                        .sorted().collect(Collectors.toList()))


        // case_id_collision_sid_fid_has_more_than_one_file

        // assert non insdc variant is not deleted
        nonInsdcVariantList = variantsColl.find(Filters.eq("_id", "A3_33333333_A_G")).into([])
        assertEquals(1, nonInsdcVariantList.size())
        insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001380.3_33333333_A_G")).into([])
        assertEquals(1, insdcVariantList.size())

        // assert the issue is logged in the file
        File variantsWithIssuesFile = new File(Paths.get(workingDir, "Variants_With_Issues", dbName + ".txt").toString())
        assertTrue(variantsWithIssuesFile.exists())
        try (BufferedReader variantsWithIssuesFileReader = new BufferedReader(new FileReader(variantsWithIssuesFile))) {
            assertEquals("A3_33333333_A_G,CM001380.3_33333333_A_G,Can't merge as sid fid common pair has more than 1 entry in file", variantsWithIssuesFileReader.readLine())
        }


        // case id collision - common sid fid has just one file

        //  assert non insdc variant deleted
        nonInsdcVariantList = variantsColl.find(Filters.eq("_id", "B1_44444444_A_G")).into([])
        assertEquals(0, nonInsdcVariantList.size())
        // assert insdc variant inserted
        insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001381.3_44444444_A_G")).into([])
        assertEquals(1, insdcVariantList.size())

        // assert all things updated to insdc in the updated variant
        insdcVariant = insdcVariantList.get(0)
        assertEquals('CM001381.3', insdcVariant.get("chr"))
        assertEquals('CM001381.3:g.44444444A>G', insdcVariant.get("hgvs")[0]["name"])
        assertEquals(Arrays.asList("sid41", "sid411", "sid42", "sid422"),
                ((List<Document>) insdcVariant.get('files')).stream()
                        .map(doc -> doc.get("sid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("fid41", "fid411", "fid42", "fid422"),
                ((List<Document>) insdcVariant.get('files')).stream()
                        .map(doc -> doc.get("fid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("sid41", "sid411", "sid42", "sid422"),
                ((List<Document>) insdcVariant.get('st')).stream()
                        .map(doc -> doc.get("sid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("fid41", "fid411", "fid42", "fid422"),
                ((List<Document>) insdcVariant.get('st')).stream()
                        .map(doc -> doc.get("fid"))
                        .sorted().collect(Collectors.toList()))
        assertEquals(Arrays.asList("G", "G", "G", "G"),
                ((List<Document>) insdcVariant.get('st')).stream()
                        .map(doc -> doc.get("mafAl"))
                        .sorted().collect(Collectors.toList()))
    }

    List<Document> getVariantsDataForHgvsNotPresent() {
        List<Document> variantDocumentList = new ArrayList<>()

        // case id collision - hgvs not already present
        // variant with insdc chromosome
        List<Document> hgvs51 = Arrays.asList(new Document("type", "genomic").append("name", "CM001381.3:g.44444444A>G"))
        List<Document> files51 = Arrays.asList(new Document("sid", "sid51").append("fid", "fid51"))
        List<Document> stats51 = Arrays.asList(getVariantStats("sid51", "fid51", 0.51, 0.51, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001382.3", "A", "G",
                55555555, 55555555, 1, hgvs51, files51, stats51))

        // variant with non insdc chromosome
        List<Document> hgvs52 = Arrays.asList(new Document("type", "genomic").append("name", "B2:g.55555555A>G"))
        List<Document> files52 = Arrays.asList(new Document("sid", "sid52").append("fid", "fid52"),
                new Document("sid", "sid51").append("fid", "fid51"))
        List<Document> stats52 = Arrays.asList(getVariantStats("sid52", "fid52", 0.52, 0.52, "A", "0/0"),
                getVariantStats("sid51", "fid51", 0.51, 0.51, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "B2", "A", "G",
                55555555, 55555555, 1, hgvs52, files52, stats52))

        // variant with insdc chromosome
        List<Document> hgvs61 = Arrays.asList(new Document("type", "genomic").append("name", "CM001382.3:g.55555555A>G"))
        List<Document> files61 = Arrays.asList(new Document("sid", "sid61").append("fid", "fid61"))
        List<Document> stats61 = Arrays.asList(getVariantStats("sid61", "fid61", 0.61, 0.61, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001383.3", "A", "G",
                66666666, 66666666, 1, hgvs61, files61, stats61))

        // variant with non insdc chromosome
        List<Document> hgvs62 = Arrays.asList(new Document("type", "genomic").append("name", "B3:g.66666666A>G"))
        List<Document> files62 = Arrays.asList(new Document("sid", "sid62").append("fid", "fid62"))
        List<Document> stats62 = Arrays.asList(getVariantStats("sid62", "fid62", 0.62, 0.62, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "B3", "A", "G",
                66666666, 66666666, 1, hgvs62, files62, stats62))

        return variantDocumentList
    }

    def qcTestNoHgvsPresent(MongoTemplate mongoTemplate, String workingDir, String testDbName) {
        MongoCollection<VariantDocument> variantsColl = mongoTemplate.getCollection(UpdateContigApplication.VARIANTS_COLLECTION)

        List<Document> insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001382.3_55555555_A_G")).into([])
        assertEquals(1, insdcVariantList.size())

        // assert all things updated to insdc in the updated variant
        Document insdcVariant = insdcVariantList.get(0)
        assertEquals(Arrays.asList("CM001381.3:g.44444444A>G", "[CM001382.3:g.55555555A>G]"),
                Arrays.asList(insdcVariant.get('hgvs')[0]['name'], insdcVariant.get('hgvs')[1]['name'])
                        .stream().sorted().collect(Collectors.toList()))

        insdcVariantList = variantsColl.find(Filters.eq("_id", "CM001383.3_66666666_A_G")).into([])
        assertEquals(1, insdcVariantList.size())

        // assert all things updated to insdc in the updated variant
        insdcVariant = insdcVariantList.get(0)
        assertEquals(Arrays.asList("CM001382.3:g.55555555A>G", "[CM001383.3:g.66666666A>G]"),
                Arrays.asList(insdcVariant.get('hgvs')[0]['name'], insdcVariant.get('hgvs')[1]['name'])
                        .stream().sorted().collect(Collectors.toList()))
    }

    List<Document> getFilesDataForStatsCalculation() {
        return Arrays.asList(
                new Document("sid", "sid211").append("fid", "fid211").append("fname", "fname211")
                        .append("samp", new Document("samp211", 0).append("samp212", 1).append("samp213", 2)),
                // multiple entries for fid212 in the files collection
                new Document("sid", "sid211").append("fid", "fid212").append("fname", "fname2121")
                        .append("samp", new Document("samp121", 0).append("samp122", 1)).append("samp123", 2),
                new Document("sid", "sid211").append("fid", "fid212").append("fname", "fname2122")
                        .append("samp", new Document("samp121", 0).append("samp122", 1)).append("samp123", 2),

                new Document("sid", "sid311").append("fid", "fid311").append("fname", "fname311")
                        .append("samp", new Document("samp311", 0).append("samp312", 1).append("samp313", 2)),

                new Document("sid", "sid411").append("fid", "fid411").append("fname", "fname411")
                        .append("samp", new Document("samp411", 0).append("samp412", 1).append("samp413", 2)),
                // multiple entries for fid412 in the files collection
                new Document("sid", "sid411").append("fid", "fid412").append("fname", "fname4121")
                        .append("samp", new Document("samp421", 0).append("samp422", 1)).append("samp423", 2),
                new Document("sid", "sid411").append("fid", "fid412").append("fname", "fname4122")
                        .append("samp", new Document("samp421", 0).append("samp422", 1)).append("samp423", 2),

        )
    }

    List<Document> getVariantsDataForStatsCalculation() {
        List<Document> variantDocumentList = new ArrayList<>()

        // case id collision - all sid and fid are different
        // variant with insdc chromosome
        List<Document> hgvs21 = Arrays.asList(new Document("type", "genomic").append("name", "CM001379.3:g.22222222A>G"))
        List<Document> files21 = Arrays.asList(
                // should calculate stats for this
                new Document("sid", "sid211").append("fid", "fid211")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                // should not calculate stats for this as no entry for sid411 in the files collections
                new Document("sid", "sid611").append("fid", "fid611")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        // variant stats has random entries - should be replaced with sensible calculated values
        List<Document> stats21 = Arrays.asList(getVariantStats("sid211", "fid211", 999, 999, "A", "0/0"),
                getVariantStats("sid311", "fid311", 999, 999, "A", "0/0"),
                getVariantStats("sid611", "fid611", 999, 999, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001379.3", "A", "G",
                22222222, 22222222, 1, hgvs21, files21, stats21))

        // variant with non insdc chromosome
        List<Document> hgvs22 = Arrays.asList(new Document("type", "genomic").append("name", "A2:g.22222222A>G"))
        List<Document> files22 = Arrays.asList(
                // should calculate stats for this
                new Document("sid", "sid311").append("fid", "fid311")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                // should not calculate for this as sid 211 and fid 212 has multiple entries in the files collection
                new Document("sid", "sid211").append("fid", "fid212")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        List<Document> stats22 = Arrays.asList(getVariantStats("sid311", "fid311", 888, 888, "A", "0/0"),
                getVariantStats("sid312", "fid312", 999, 999, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "A2", "A", "G",
                22222222, 22222222, 1, hgvs22, files22, stats22))


        // case id collision - all sid and fid are different
        // variant with insdc chromosome
        List<Document> hgvs41 = Arrays.asList(new Document("type", "genomic").append("name", "CM001381.3:g.44444444A>G"))
        List<Document> files41 = Arrays.asList(
                // should calculate stats for this
                new Document("sid", "sid411").append("fid", "fid411")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                // should not calculate stats for this as no entry for sid811 in the files collections
                new Document("sid", "sid811").append("fid", "fid811")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        // variant stats has random entries - should be replaced with sensible calculated values
        List<Document> stats41 = Arrays.asList(getVariantStats("sid411", "fid411", 999, 999, "A", "0/0"),
                getVariantStats("sid411", "fid411", 999, 999, "A", "0/0"),
                getVariantStats("sid611", "fid611", 999, 999, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "CM001381.3", "A", "G",
                44444444, 44444444, 1, hgvs41, files41, stats41))

        // variant with non insdc chromosome
        List<Document> hgvs42 = Arrays.asList(new Document("type", "genomic").append("name", "B1:g.44444444A>G"))
        List<Document> files42 = Arrays.asList(
                // should calculate stats for this
                new Document("sid", "sid311").append("fid", "fid311")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                // should not calculate for this as sid 211 and fid 212 has multiple entries in the files collection
                new Document("sid", "sid411").append("fid", "fid412")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))),
                new Document("sid", "sid411").append("fid", "fid411")
                        .append("samp", new Document("def", "0|0").append("0|1", Arrays.asList(1))))
        List<Document> stats42 = Arrays.asList(getVariantStats("sid411", "fid411", 888, 888, "A", "0/0"),
                getVariantStats("sid412", "fid412", 999, 999, "A", "0/0"))
        variantDocumentList.add(getVariantDocument(Variant.VariantType.SNV.toString(), "B1", "A", "G",
                44444444, 44444444, 1, hgvs42, files42, stats42))


        return variantDocumentList
    }


    void qcTestWithStatsCalculation(MongoTemplate mongoTemplate, String workingDir, String dbName) {
        // case merge all sid fid are different
        List<VariantDocument> variantsList = mongoTemplate.getCollection(UpdateContigApplication.VARIANTS_COLLECTION)
                .find(Filters.eq("_id", buildVariantId("CM001379.3", 22222222, "A", "G"))).into([])
                .stream().map(doc -> mongoTemplate.getConverter().read(VariantDocument.class, doc))
                .collect(Collectors.toList())
        assertEquals(1, variantsList.size())

        Set<VariantStatsMongo> variantStatsList = variantsList.get(0).getVariantStatsMongo()
        assertEquals(2, variantStatsList.size())

        // assert the newly calculated stats
        VariantStatsMongo variantStatsForSid211Fid211 = variantStatsList.stream()
                .filter(st -> st.getStudyId().equals("sid211") && st.getFileId().equals("fid211"))
                .findFirst().get()
        Map<String, Integer> numOfGT = variantStatsForSid211Fid211.getNumGt()
        assertEquals(2, numOfGT.get("0|0"))
        assertEquals(1, numOfGT.get("0|1"))
        assertEquals(0.1666666716337204f, variantStatsForSid211Fid211.getMaf())
        assertEquals(0.3333333432674408f, variantStatsForSid211Fid211.getMgf())
        assertEquals("G", variantStatsForSid211Fid211.getMafAllele())
        assertEquals("0|1", variantStatsForSid211Fid211.getMgfGenotype())
        assertEquals(0, variantStatsForSid211Fid211.getMissingAlleles())
        assertEquals(0, variantStatsForSid211Fid211.getMissingGenotypes())

        VariantStatsMongo variantStatsForSid311Fid311 = variantStatsList.stream()
                .filter(st -> st.getStudyId().equals("sid311") && st.getFileId().equals("fid311"))
                .findFirst().get()
        numOfGT = variantStatsForSid311Fid311.getNumGt()
        assertEquals(2, numOfGT.get("0|0"))
        assertEquals(1, numOfGT.get("0|1"))
        assertEquals(0.1666666716337204f, variantStatsForSid311Fid311.getMaf())
        assertEquals(0.3333333432674408f, variantStatsForSid311Fid311.getMgf())
        assertEquals("G", variantStatsForSid311Fid311.getMafAllele())
        assertEquals("0|1", variantStatsForSid311Fid311.getMgfGenotype())
        assertEquals(0, variantStatsForSid311Fid311.getMissingAlleles())
        assertEquals(0, variantStatsForSid311Fid311.getMissingGenotypes())


        // case merge common sid fid has only one entry in the files collections

        variantsList = mongoTemplate.getCollection(UpdateContigApplication.VARIANTS_COLLECTION)
                .find(Filters.eq("_id", buildVariantId("CM001381.3", 44444444, "A", "G"))).into([])
                .stream().map(doc -> mongoTemplate.getConverter().read(VariantDocument.class, doc))
                .collect(Collectors.toList())
        assertEquals(1, variantsList.size())

        variantStatsList = variantsList.get(0).getVariantStatsMongo()
        assertEquals(2, variantStatsList.size())

        // assert the newly calculated stats
        VariantStatsMongo variantStatsForSid411Fid411 = variantStatsList.stream()
                .filter(st -> st.getStudyId().equals("sid411") && st.getFileId().equals("fid411"))
                .findFirst().get()
        numOfGT = variantStatsForSid411Fid411.getNumGt()
        assertEquals(2, numOfGT.get("0|0"))
        assertEquals(1, numOfGT.get("0|1"))
        assertEquals(0.1666666716337204f, variantStatsForSid411Fid411.getMaf())
        assertEquals(0.3333333432674408f, variantStatsForSid411Fid411.getMgf())
        assertEquals("G", variantStatsForSid411Fid411.getMafAllele())
        assertEquals("0|1", variantStatsForSid411Fid411.getMgfGenotype())
        assertEquals(0, variantStatsForSid411Fid411.getMissingAlleles())
        assertEquals(0, variantStatsForSid411Fid411.getMissingGenotypes())

        variantStatsForSid311Fid311 = variantStatsList.stream()
                .filter(st -> st.getStudyId().equals("sid311") && st.getFileId().equals("fid311"))
                .findFirst().get()
        numOfGT = variantStatsForSid311Fid311.getNumGt()
        assertEquals(2, numOfGT.get("0|0"))
        assertEquals(1, numOfGT.get("0|1"))
        assertEquals(0.1666666716337204f, variantStatsForSid311Fid311.getMaf())
        assertEquals(0.3333333432674408f, variantStatsForSid311Fid311.getMgf())
        assertEquals("G", variantStatsForSid311Fid311.getMafAllele())
        assertEquals("0|1", variantStatsForSid311Fid311.getMgfGenotype())
        assertEquals(0, variantStatsForSid311Fid311.getMissingAlleles())
        assertEquals(0, variantStatsForSid311Fid311.getMissingGenotypes())
    }

    Document getFileDocument(String sid, String fid, String fileName) {
        return new Document().append("sid", sid).append("fid", fid).append("fname", fileName)
    }

    Document getVariantDocument(String variantType, String chromosome, String ref, String alt, int start,
                                int end, int length, List<Document> hgvs, List<Document> files, List<Document> stats) {
        return new Document()
                .append("_id", buildVariantId(chromosome, start, ref, alt))
                .append("type", variantType)
                .append("chr", chromosome)
                .append("ref", ref)
                .append("alt", alt)
                .append("start", start)
                .append("end", end)
                .append("len", length)
                .append("hgvs", hgvs)
                .append("files", files)
                .append("st", stats)
    }

    Document getVariantStats(String studyId, String fileId, float maf, float mgf, String mafAl, String mgfAl) {
        return new Document()
                .append("sid", studyId)
                .append("fid", fileId)
                .append("maf", maf)
                .append("mgf", mgf)
                .append("mafAl", mafAl)
                .append("mgfGt", mgfAl)
    }

    String buildVariantId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome)
        builder.append("_")
        builder.append(start)
        builder.append("_")
        if (!reference.equals("-")) {
            if (reference.length() < 50) {
                builder.append(reference)
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(reference)))
            }
        }

        builder.append("_")
        if (!alternate.equals("-")) {
            if (alternate.length() < 50) {
                builder.append(alternate)
            } else {
                builder.append(new String(CryptoUtils.encryptSha1(alternate)))
            }
        }
        return builder.toString()
    }

}