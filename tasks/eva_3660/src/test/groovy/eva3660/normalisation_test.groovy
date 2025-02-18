package eva3660

import org.junit.After
import org.junit.Before
import org.junit.Test

import java.nio.file.Paths

import static org.junit.Assert.assertEquals


class NormalisationTest {

    private NormalisationProcessor processor

    @Before
    void setUp() throws Exception {
        processor = new NormalisationProcessor(Paths.get(
                NormalisationTest.class.getResource("/fasta/homo_sapiens/GCA_000001405.29/GCA_000001405.29.fa").toURI()))
    }

    @After
    void tearDown() throws Exception {
        processor.close()
    }

    @Test
    void testNormalise() {
        // Section of chr21 consisting of ATTT repeats
        assertEquals("TTTTATTTATTTATTTATTTATTTATTTATTTATTTATTT",
                processor.fastaReader.getSequenceToUpperCase("CM000683.2", 7678481, 7678480 + (10*4)))

        // Variant that deletes one repeat from the middle is normalised to be left-aligned
        // In VCF: TAG=Version1
        assertEquals(new Tuple(7678481, 7678485, 5, ["TTTTA", "T"]),
                processor.normalise("CM000683.2", 7678489, 7678496, 8, ["ATTTATTT", "ATTT"]))

        // Same variant with different representation (empty allele)
        // In VCF: TAG=Version2
        assertEquals(new Tuple(7678481, 7678485, 5, ["TTTTA", "T"]),
                processor.normalise("CM000683.2", 7678489, 7678492, 4, ["ATTT", ""]))

        // Multiple alternate alleles
        // In VCF: TAG=Multi1
        assertEquals(new Tuple(7678481, 7678489, 9, ["TTTTA", "T", "TTTTATTTA"]),
                processor.normalise("CM000683.2", 7678489, 7678500, 12, ["ATTTATTT", "ATTT", "ATTTATTTATTT"]))

        // Different set of alternate alleles has different result
        // In VCF: TAG=Multi2
        assertEquals(new Tuple(7678489, 7678496, 8, ["ATTTATTT", "ATTT", "C"]),
                processor.normalise("CM000683.2", 7678489, 7678496, 8, ["ATTTATTT", "ATTT", "C"]))

        // Variant at position 1
        // In VCF: TAG=Position1
        assertEquals(new Tuple(1, 1, 1, ["N", "C"]),
                processor.normalise("CM000683.2", 1, 2, 2, ["NN", "CN"]))

        // Empty allele at position 1
        // In VCF: TAG=Position2
        assertEquals(new Tuple(1, 2, 2, ["", "AT"]),
                processor.normalise("CM000683.2", 1, 2, 2, ["", "AT"]))

        // Run bcftools norm to compare
        runBcfToolsNorm()
    }

    @Test
    void testNormaliseAndTruncate() {
        def input = new ValuesForNormalisation(7678489, 7678500, 12, "ATTTATTT", "ATTT",["ATTTATTTATTT"])
        // Initial T is truncated
        def expectedOutput = new ValuesForNormalisation(7678482, 7678489, 8, "TTTA", "", ["TTTATTTA"])
        assertEquals(expectedOutput, processor.normaliseAndTruncate("CM000683.2", input))

        // Allows null mafAllele and no secondary alternates
        input = new ValuesForNormalisation(7678489, 7678500, 12, "ATTTATTT", "ATTT", [])
        expectedOutput = new ValuesForNormalisation(7678482, 7678489, 8, "TTTA", "", [])
        assertEquals(expectedOutput, processor.normaliseAndTruncate("CM000683.2", input))
    }

    void runBcfToolsNorm() {
        def fastaPath = NormalisationTest.class.getResource("/fasta/homo_sapiens/GCA_000001405.29/GCA_000001405.29.fa").toString().substring(5)
        def inputVcfPath = NormalisationTest.class.getResource("/test_before_norm.vcf").toString().substring(5)
        def outputVcfPath = "test_after_norm.vcf"

        // Run locally with bcftools in path
        def command = "bcftools norm --no-version -cw -f ${fastaPath} -O z -o ${outputVcfPath} ${inputVcfPath}"
        runProcess(command)
    }

    Object runProcess(String command) {
        println("Running $command...")
        def process = ["bash", "-c", command].execute()
        def (out, err) = [new StringBuffer(), new StringBuffer()]
        process.waitForProcessOutput(out, err)
        if (!out.toString().trim().equals("")) println(out.toString())
        if (process.exitValue() != 0) {
            println("Command: $command exited with exit code ${process.exitValue()}!!")
            println("See error messages below: \n" + err.toString())
            throw new Exception(err.toString())
        }
        if (!err.toString().trim().equals("")) println(err.toString())
        return ["out": out.toString(), "err": err.toString()]
    }

}