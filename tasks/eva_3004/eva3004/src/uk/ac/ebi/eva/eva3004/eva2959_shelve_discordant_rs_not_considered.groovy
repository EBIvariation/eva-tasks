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

def prodEnv = createFromSpringContext(prodPropertiesFile, Application.class)

// RSs whose locus should be corrected but were not picked up for processing by EVA-2959
// because they were logged for position mismatch in EVA-2706
// ex: grep -n 53195563 /nfs/production/keane/eva/tasks/EVA2706/GCA_001433935.1_errors.log
def allRSToCheck = ["bash", "-c",
                    "ls -1 ${discordantRSDir}/*errors.log"].execute().text.split("\n").collectEntries{
    [it.split("/").reverse()[0].split("_errors")[0],
     ["bash", "-c", "grep \"positions found in submitted variants\" ${it} | grep -o -E \"for rs[0-9]+\" " +
             "| grep -o -E \"[0-9]+\""].execute().text.trim().split("\n").findAll{!it.empty}.collect{it.toLong()}.toSet()
    ]}
// 4,735,719
println(allRSToCheck.values().flatten().size())
def shelvedCollectionDbsnpSve = "eva2706_shelved_dbsnpsve_multi_position_rs"
def shelvedCollectionSve = "eva2706_shelved_sve_multi_position_rs"
def batchIndex = 0
allRSToCheck.each{assembly, allRSIDs -> allRSIDs.collate(1000).each { rsIDs ->
    def shelvedDbsnpSveBulkOps = prodEnv.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, dbsnpSveClass, shelvedCollectionDbsnpSve)
    def shelvedSveBulkOps = prodEnv.mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, sveClass, shelvedCollectionSve)
    def allSvesWithRS = [sveClass, dbsnpSveClass].collect{prodEnv.mongoTemplate.find(query(where("seq").is(assembly)
            .and("rs").in(rsIDs)), it)}.flatten()
    // Ensure that the RS reported with multiple positions among the SS still holds true in the current SVE collections
    def allSvesToShelve = allSvesWithRS.groupBy{it.clusteredVariantAccession}.findAll{k, v ->
        v.unique{toClusteredVariantEntity(it).hashedMessage}.size() > 1}.values().flatten()
    def dbsnpSvesToShelve = allSvesToShelve.findAll{it.accession < 5e9}
    def evaSvesToShelve = allSvesToShelve.findAll{it.accession >= 5e9}
    try {
        if (dbsnpSvesToShelve.size() > 0) {
            shelvedDbsnpSveBulkOps.insert(dbsnpSvesToShelve)
            shelvedDbsnpSveBulkOps.execute()
        }
        if (evaSvesToShelve.size() > 0) {
            shelvedSveBulkOps.insert(evaSvesToShelve)
            shelvedSveBulkOps.execute()
        }
    } catch (DuplicateKeyException ignored) {}

    println("Shelved ${dbsnpSvesToShelve.size()} dbsnp SVEs in batch ${batchIndex}...")
    println("Shelved ${evaSvesToShelve.size()} EVA SVEs in batch ${batchIndex}...")
    batchIndex += 1
}}
//478,810
println(prodEnv.mongoTemplate.count(new Query(), sveClass, shelvedCollectionSve))
//21,657,719
println(prodEnv.mongoTemplate.count(new Query(), dbsnpSveClass, shelvedCollectionDbsnpSve))
