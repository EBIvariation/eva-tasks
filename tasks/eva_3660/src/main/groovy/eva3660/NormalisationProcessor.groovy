package eva3660

import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader

import java.nio.file.Path

class NormalisationProcessor {

    private FastaSequenceReader fastaReader

    NormalisationProcessor(Path pathToFasta) {
        fastaReader = new FastaSequenceReader(pathToFasta)
    }

    /**
     * Normalisation with specified reference, alternate, secondary alternate alleles, and maf allele.
     * Also truncate common leading context allele if present (i.e. allows empty alleles).
     *
     * @param contig Name of contig as found in FASTA
     * @param valsforNorm Values required for normalisation (position & various alleles) Position of variant
     * @return normalised values
     */
    ValuesForNormalisation normaliseAndTruncate(String contig, ValuesForNormalisation valsForNorm) {
        List<String> allelesToNorm = [valsForNorm.reference, valsForNorm.alternate] + valsForNorm.secondaryAlternates
        def (newStart, newEnd, newLength, newAlleles) = normalise(contig, valsForNorm.start, valsForNorm.end, valsForNorm.length, allelesToNorm)
        // Check final base before initial base when truncating, to mirror eva-pipeline load:
        //  https://github.com/EBIvariation/eva-pipeline/blob/master/src/main/java/uk/ac/ebi/eva/pipeline/io/mappers/VariantVcfFactory.java#L185
        if (allSameEnd(newAlleles)) {
            (newEnd, newLength, newAlleles) = truncateRightmost(newEnd, newLength, newAlleles)
        }
        else if (allSameStart(newAlleles)) {
            (newStart, newLength, newAlleles) = truncateLeftmost(newStart, newLength, newAlleles)
        }
        String newReference = newAlleles.pop()
        String newAlternate = newAlleles.pop()
        return new ValuesForNormalisation(newStart, newEnd, newLength, newReference, newAlternate, newAlleles)
    }

    /**
     * Normalise alleles to be parsimonious and left-aligned.
     * See here: https://genome.sph.umich.edu/wiki/Variant_Normalization
     *
     * @param contig Name of contig as found in FASTA
     * @param start Start coordinate of variant
     * @param end End coordinate of variant
     * @param length Length of variant
     * @param alleles List of alleles
     * @return normalised coordinates and list of normalised alleles (guaranteed to preserve input order)
     */
    Tuple normalise(String contig, int start, int end, int length, List<String> alleles) {
        // Allow for initially empty alleles
        def (newStart, newLength, newAlleles) = addContextIfEmpty(contig, start, length, alleles)
        def newEnd = end
        // While all alleles end in same nucleotide
        while (allSameEnd(newAlleles)) {
            // Truncate rightmost nucleotide
            (newEnd, newLength, newAlleles) = truncateRightmost(newEnd, newLength, newAlleles)
            // If exists an empty allele, extend alleles 1 to the left
            (newStart, newLength, newAlleles) = addContextIfEmpty(contig, newStart, newLength, newAlleles)
        }
        // While all alleles start with same nucleotide and have length 2 or more
        while (allSameStart(newAlleles) && allLengthAtLeastTwo(newAlleles)) {
            // Truncate leftmost nucleotide
            (newStart, newLength, newAlleles) = truncateLeftmost(newStart, newLength, newAlleles)
        }
        return new Tuple(newStart, newEnd, newLength, newAlleles)
    }

    private Tuple addContextIfEmpty(String contig, int start, int length, List<String> alleles) {
        def newStart = start
        def newLength = length
        def newAlleles = alleles
        // If already at the edge of the contig, do nothing
        if (start == 1) {
            return new Tuple(newStart, newLength, newAlleles)
        }
        def existEmptyAlleles = alleles.stream().any{ it.size() < 1 }
        if (existEmptyAlleles) {
            // Extend alleles 1 to the left
            newStart--
            newLength++
            def contextBase = fastaReader.getSequenceToUpperCase(contig, newStart, newStart)
            newAlleles = newAlleles.stream().collect { "${contextBase}${it}" }
        }
        return new Tuple(newStart, newLength, newAlleles)
    }

    private boolean allSameEnd(List<String> alleles) {
        return alleles.stream().noneMatch { it.size() == 0 }
                && alleles.stream().collect{it[-1] }.toSet().size() == 1
    }

    private boolean allSameStart(List<String> alleles) {
        return alleles.stream().noneMatch { it.size() == 0 }
                && alleles.stream().collect{it[0] }.toSet().size() == 1
    }

    private boolean allLengthAtLeastTwo(List<String> alleles) {
        return alleles.stream().allMatch{it.size() >= 2 }
    }

    private Tuple truncateRightmost(int end, int length, List<String> alleles) {
        return new Tuple(--end, --length, alleles.stream().collect{ it.substring(0, it.size()-1) })
    }

    private Tuple truncateLeftmost(int start, int length, List<String> alleles) {
        return new Tuple(++start, --length, alleles.stream().collect{ it.substring(1) })
    }

    void close() {
        fastaReader.close()
    }

}
