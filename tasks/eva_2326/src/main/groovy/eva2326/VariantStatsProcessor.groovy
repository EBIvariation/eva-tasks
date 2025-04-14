package eva2326

import com.mongodb.BasicDBObject
import org.opencb.biodata.models.feature.Genotype
import uk.ac.ebi.eva.commons.models.data.VariantStats
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantSourceEntryMongo
import uk.ac.ebi.eva.commons.models.mongo.entity.subdocuments.VariantStatsMongo

import java.util.stream.Collectors

// Taken from eva-pipeline with minor changes to make it work directly in eva-tasks (no functional/logic changes)
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
