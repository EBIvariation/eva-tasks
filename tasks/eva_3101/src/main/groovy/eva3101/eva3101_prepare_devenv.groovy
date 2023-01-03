package eva3101

import groovy.cli.picocli.CliBuilder
import org.springframework.batch.core.JobParameter
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.StepExecutionListener
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.item.ItemStreamException
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.NonTransientResourceException
import org.springframework.batch.item.ParseException
import org.springframework.batch.item.UnexpectedInputException
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy
import org.springframework.transaction.PlatformTransactionManager
import uk.ac.ebi.eva.accession.core.batch.io.SubmittedVariantDeprecationWriter
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity
import uk.ac.ebi.eva.accession.deprecate.Application
import uk.ac.ebi.eva.accession.deprecate.batch.listeners.DeprecationStepProgressListener
import uk.ac.ebi.eva.groovy.commons.RetryableBatchingCursor
import uk.ac.ebi.eva.metrics.metric.MetricCompute

import java.time.LocalDateTime

import static uk.ac.ebi.eva.groovy.commons.EVADatabaseEnvironment.*
import static org.springframework.data.mongodb.core.query.Criteria.where

// This script deprecates variants with allelesMatch attribute in dbsnpSVE collection and also the corresponding RS (if any)
def cli = new CliBuilder()
cli.prodPropertiesFile(args: 1, "Production properties file for accessioning", required: true)
cli.devPropertiesFile(args: 1, "Development properties file for accessioning", required: true)
cli.options.assemblyToDeprecate(args: 1, "Assembly to be deprecated", required: true)
def options = cli.parse(args)
if (!options) {
    cli.usage()
    System.exit(1)
}

def prodEnv = createFromSpringContext(options.prodPropertiesFile, Application.class)
def devEnv = createFromSpringContext(options.devPropertiesFile, Application.class)

// Transfer data to DEV for an assembly
def ssEntryBatches = new RetryableBatchingCursor(where("seq").is(options.assemblyToDeprecate).and("allelesMatch").exists(true),
        prodEnv.mongoTemplate, dbsnpSveClass)
ssEntryBatches.each { ssEntries ->
    devEnv.bulkInsertIgnoreDuplicates(ssEntries, dbsnpSveClass)
    List<Long> correspondingRSIDs = ssEntries.collect { it.clusteredVariantAccession }.findAll {Objects.nonNull(it)}
    List<String> correspondingSSHashes = ssEntries.collect { it.hashedMessage }
    def correspondingRSEntries = new RetryableBatchingCursor(where("asm").is(options.assemblyToDeprecate)
            .and("accession").in(correspondingRSIDs), prodEnv.mongoTemplate,
            dbsnpCveClass).collect()
    def otherSSEntriesWithSameRS = new RetryableBatchingCursor(where("seq").is(options.assemblyToDeprecate)
            .and("rs").in(correspondingRSIDs).and("_id").nin(correspondingSSHashes),
            prodEnv.mongoTemplate, dbsnpSveClass).collect()
    devEnv.bulkInsertIgnoreDuplicates(correspondingRSEntries, dbsnpCveClass)
    devEnv.bulkInsertIgnoreDuplicates(otherSSEntriesWithSameRS, dbsnpSveClass)
}
