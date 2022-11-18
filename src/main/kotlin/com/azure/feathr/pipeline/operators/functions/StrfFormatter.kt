package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.InvalidDateFormatException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.format.FormatStyle
import java.util.*


/**
 * Datetime format string formatter, supporting both python and java compatible format strings by converting any percent-tokens from python into their java equivalents.
 *
 * Taken from https://github.com/HubSpot/jinjava/blob/master/src/main/java/com/hubspot/jinjava/objects/date/StrftimeFormatter.java
 * Translated into Kotlin
 * License: Apache 2.0
 *
 * @author jstehler
 */
typealias Append = (
    builder: DateTimeFormatterBuilder,
    stripLeadingZero: Boolean
) -> DateTimeFormatterBuilder


object StrftimeFormatter {
    private const val DEFAULT_DATE_FORMAT = "%H:%M / %d-%m-%Y"

    /*
   * Mapped from http://strftime.org/, http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
   */
    private val COMPONENTS = mapOf(
        'a' to pattern("EEE"),
        'A' to pattern("EEEE"),
        'b' to pattern("MMM"),
        'B' to pattern("MMMM"),
        'c' to localized(FormatStyle.MEDIUM, FormatStyle.MEDIUM),
        'd' to pattern("dd"),
        'e' to pattern("d"), // The day of the month like with %d, but padded with blank (range 1 through 31),
        'f' to pattern("SSSSSS"),
        'H' to pattern("HH"),
        'h' to pattern("hh"),
        'I' to pattern("hh"),
        'j' to pattern("DDD"),
        'k' to pattern("H"), // The hour as a decimal number, using a 24-hour clock like %H, but padded with blank (range 0 through 23)
        'l' to pattern("h"), // The hour as a decimal number, using a 12-hour clock like %I, but padded with blank (range 1 through 12)
        'm' to pattern("MM"),
        'M' to pattern("mm"),
        'p' to pattern("a"),
        'S' to pattern("ss"),
        'U' to pattern("ww"),
        'w' to pattern("e"),
        'W' to pattern("ww"),
        'x' to localized(FormatStyle.SHORT, null),
        'X' to localized(null, FormatStyle.MEDIUM),
        'y' to pattern("yy"),
        'Y' to pattern("yyyy"),
        'z' to pattern("Z"),
        'Z' to pattern("z"),
        '%' to { b, _ ->
            b.appendLiteral("%")
        }
    )

    private val NOMINATIVE_COMPONENTS = mapOf('B' to pattern("LLLL"))

    /**
     * Build a [DateTimeFormatter] that matches the given Python `strftime` pattern.
     *
     * @see [Python `strftime` cheatsheet](https://strftime.org/)
     */
    fun toDateTimeFormatter(strftime: String): DateTimeFormatter {
        if (!strftime.contains('%')) {
            return DateTimeFormatter.ofPattern(strftime)
        }
        val builder = DateTimeFormatterBuilder()
        var i = 0
        while (i < strftime.length) {
            var c = strftime[i]
            if (c != '%' || strftime.length <= i + 1) {
                builder.appendLiteral(c)
                i++
                continue
            }
            c = strftime[++i]
            var stripLeadingZero = false
            if (c == '-') {
                stripLeadingZero = true
                c = strftime[++i]
            }
            var components = COMPONENTS
            if (c == 'O') {
                c = strftime[++i]
                components = NOMINATIVE_COMPONENTS
            }
            val finalChar = c
            (components[c] ?: throw InvalidDateFormatException(
                strftime, String.format("unknown format code '%s'", finalChar)
            ))(builder, stripLeadingZero)
            i++
        }
        return builder.toFormatter()
    }

    private fun formatter(strftime: String, locale: Locale?): DateTimeFormatter {
        val fmt: DateTimeFormatter = when (strftime.lowercase(Locale.getDefault())) {
            "short" -> DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
            "medium" -> DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
            "long" -> DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG)
            "full" -> DateTimeFormatter.ofLocalizedDateTime(FormatStyle.FULL)
            else -> try {
                toDateTimeFormatter(strftime)
            } catch (e: IllegalArgumentException) {
                throw InvalidDateFormatException(strftime, e)
            }
        }
        return fmt.withLocale(locale)
    }

    fun format(d: ZonedDateTime?, locale: Locale?): String {
        return format(d, DEFAULT_DATE_FORMAT, locale)
    }

    @JvmOverloads
    fun format(d: ZonedDateTime?, strftime: String = DEFAULT_DATE_FORMAT, locale: Locale? = Locale.ENGLISH): String {
        return formatter(strftime, locale).format(d)
    }

    private fun pattern(targetPattern: String): Append {
        return { b, s ->
            b.appendPattern(
                if (s) targetPattern.substring(1) else targetPattern
            )
        }
    }

    private fun localized(dateStyle: FormatStyle?, timeStyle: FormatStyle?): Append {
        return { b, _ ->
            b.appendLocalized(
                dateStyle,
                timeStyle
            )
        }
    }
}
