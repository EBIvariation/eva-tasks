package uk.ac.ebi.eva.eva3004

import groovy.cli.picocli.CliBuilder
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.query.Query
import uk.ac.ebi.eva.accession.deprecate.Application

import static uk.ac.ebi.eva.eva3004.EVADatabaseEnvironment.*
import static org.springframework.data.mongodb.core.query.Criteria.where
import static org.springframework.data.mongodb.core.query.Query.query

def cli = new CliBuilder()
cli.prodPropertiesFile(args:1, longOpt: "prod-props-file", "Production properties file for accessioning",  required: true)
cli.discordantRSDir(args:1, longOpt: "eva2706-discordant-rs-dir", "Directory containing lists of discordant RS " +
        "for each assembly (from EVA-2706)",  required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}
def (prodPropertiesFile, discordantRSDir) = [options.prodPropertiesFile, options.discordantRSDir]
def scriptLogger = EVALoggingUtils.getLogger(this.class)

def prodEnv = createFromSpringContext(prodPropertiesFile, Application.class)

// All RS from both EVA-1771 and EVA-2706 (files from the former are symlinked in the tasks directory for the latter)
def allRSToCheck = ["bash", "-c",
                    "ls -1 ${discordantRSDir}/*errors.log"].execute().text.split("\n").collectEntries{
    [it.split("/").reverse()[0].split("_errors")[0],
     ["bash", "-c", "grep \"cluster position\" ${it} | grep -o -E \"for rs[0-9]+\" " +
            "| grep -o -E \"[0-9]+\""].execute().text.trim().split("\n").findAll{!it.empty}.collect{it.toLong()}.toSet()
    ]}
// 6,629,703
println(allRSToCheck.values().flatten().size())
// If some of these RS IDs were merged into others include those too
prodEnv.mongoTemplate.find(query(where("_id").regex("EVA2850_MERGED.*")), dbsnpCvoeClass).each{
    allRSToCheck[it.inactiveObjects[0].assemblyAccession].addAll([it.accession, it.mergedInto].toSet())
}
def batchIndex = 0
def shelvedCollectionDbsnpSve = "eva2959_shelved_dbsnpsve"
def shelvedCollectionSve = "eva2959_shelved_sve"
// Check if every SVE has an RS with a consistent locus
allRSToCheck.each{assembly, allRSIDs -> allRSIDs.collate(1000).each{rsIDs ->
    def shelvedDbsnpSveBulkOps = prodEnv.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, dbsnpSveClass, shelvedCollectionDbsnpSve)
    def shelvedSveBulkOps = prodEnv.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, sveClass, shelvedCollectionSve)
    def sves = [sveClass, dbsnpSveClass].collect{
        prodEnv.mongoTemplate.find(query(where("seq").is(assembly).and("rs").in(rsIDs)), it)}.flatten().findAll{
        it.isAllelesMatch() && Objects.isNull(it.mapWeight)}
    def rsGroupedByHashAndAccessionFromSVE =
            sves.collectEntries{sve ->
                def cve = toClusteredVariantEntity(sve)
                return ["${cve.hashedMessage}_${cve.accession}", sve]
            }
    def rsHashesToLookUp = rsGroupedByHashAndAccessionFromSVE.keySet().collect{it.split("_")[0]}
    scriptLogger.info("Looking up ${rsHashesToLookUp.size()} RS hashes for ${assembly} in batch ${batchIndex}...")
    def rsHashesInCveGroupedByHashAndAccession =
            [cveClass, dbsnpCveClass].collect { prodEnv.mongoTemplate.find(query(where("_id").in(rsHashesToLookUp)), it) }
                    .flatten().collectEntries { ["${it.hashedMessage}_${it.accession}", it.hashedMessage] }
    def allImpactedSS = new ArrayList<>()
    (rsGroupedByHashAndAccessionFromSVE.keySet() - rsHashesInCveGroupedByHashAndAccession.keySet()).each{rsWithMismatchedLocus ->
        def (rsHash, rsAccession) = rsWithMismatchedLocus.split("_")
        def impactedSS = rsGroupedByHashAndAccessionFromSVE.get(rsWithMismatchedLocus)
        allImpactedSS.add(impactedSS)
        scriptLogger.error("SS/RS locus mismatch for SS ${impactedSS.accession} with SS hash ${impactedSS.hashedMessage} " +
                "and RS ${rsAccession} and RS hash ${rsHash}...")
        scriptLogger.info("Shelving SS ${impactedSS.accession} with SS hash ${impactedSS.hashedMessage} with  a mismatched locus...")
    }
    try {
        def evaSvesToShelve = allImpactedSS.findAll{it.accession >= 5e9}
        def dbsnpSvesToShelve = allImpactedSS.findAll{it.accession < 5e9}
        if (evaSvesToShelve.size() > 0) {
            shelvedSveBulkOps.insert(evaSvesToShelve)
            shelvedSveBulkOps.execute()
        }
        if (dbsnpSvesToShelve.size() > 0) {
            shelvedDbsnpSveBulkOps.insert(dbsnpSvesToShelve)
            shelvedDbsnpSveBulkOps.execute()
        }
    } catch (DuplicateKeyException ignored) {}

    batchIndex += 1
}}
//65
println(prodEnv.mongoTemplate.count(new Query(), sveClass, shelvedCollectionSve))
//169,512
println(prodEnv.mongoTemplate.count(new Query(), dbsnpSveClass, shelvedCollectionDbsnpSve))
