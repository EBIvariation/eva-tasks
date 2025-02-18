package eva3660

import uk.ac.ebi.eva.accession.core.contig.ContigMapping
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms

import java.nio.file.Path

import static org.springframework.util.StringUtils.hasText;


class ContigNotFoundException extends IllegalArgumentException {
    ContigNotFoundException(String message) {
        super(message)
    }
}


class ContigRenamingProcessor {

    private ContigMapping contigMapping

    private String dbName

    // Additional mappings for contigs not contained in assembly report but present in the database
    private final static Map additionalContigMappings = [
            "eva_cfamiliaris_31" : [
                    "chr1": "CM000001.3",
                    "chr2": "CM000002.3",
                    "chr3": "CM000003.3",
                    "chr4": "CM000004.3",
                    "chr5": "CM000005.3",
                    "chr6": "CM000006.3",
                    "chr7": "CM000007.3",
                    "chr8": "CM000008.3",
                    "chr9": "CM000009.3"
            ]
    ]

    ContigRenamingProcessor(Path pathToAssemblyReport, String dbName) {
        contigMapping = new ContigMapping("file:" + pathToAssemblyReport.toAbsolutePath().toString())
        this.dbName = dbName
    }

    String getInsdcAccession(String contigName) {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName)

        StringBuilder message = new StringBuilder()
        if (isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            return contigSynonyms.getGenBank()
        } else if (additionalContigMappings.contains(dbName) && additionalContigMappings[dbName].contains(contigName)) {
            return additionalContigMappings[dbName][contigName]
        }

        throw new ContigNotFoundException(message.toString())
    }

    /* More lenient version of this method in accessioning pipeline:
     * https://github.com/EBIvariation/eva-accession/blob/master/eva-accession-core/src/main/java/uk/ac/ebi/eva/accession/core/contig/ContigMapping.java#L169
     */
    boolean isGenbankReplacementPossible(String contig, ContigSynonyms contigSynonyms, StringBuilder reason) {
        if (contigSynonyms == null) {
            reason.append("Contig '" + contig + "' was not found in the assembly report!");
            return false;
        }

        if (!hasText(contigSynonyms.getGenBank())) {
            reason.append("No Genbank equivalent found for contig '" + contig + "' in the assembly report");
            return false;
        }
        return true;
    }

}
