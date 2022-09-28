package uk.ac.ebi.eva.eva3004

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.FileAppender
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EVALoggingUtils {
    static Logger getLogger(Class aClass, String fileName = "") {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory()
        PatternLayoutEncoder ple = new PatternLayoutEncoder()
        ple.setPattern("%date %level [%thread] %logger{10} [%file:%line] %msg%n")
        ple.setContext(lc)
        ple.start()
        def scriptLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(aClass)

        if (fileName != "") {
            def fileAppender = new FileAppender()
            fileAppender.setFile(fileName)
            fileAppender.setAppend(true)
            fileAppender.setEncoder(ple)
            fileAppender.setContext(lc)
            fileAppender.start()
            scriptLogger.addAppender(fileAppender)
            scriptLogger.addAppender(new ConsoleAppender<ILoggingEvent>())
            scriptLogger.setAdditive(false)
        } else {
            def consoleAppender = new ConsoleAppender()
            consoleAppender.setAppend(true)
            consoleAppender.setEncoder(ple)
            consoleAppender.setContext(lc)
            consoleAppender.start()
            scriptLogger.addAppender(consoleAppender)
            scriptLogger.setAdditive(false)
        }

        scriptLogger.setLevel(Level.INFO)
        return scriptLogger
    }
}
