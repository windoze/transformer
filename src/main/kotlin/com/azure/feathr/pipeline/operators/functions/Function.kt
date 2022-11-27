package com.azure.feathr.pipeline.operators.functions

import com.azure.feathr.pipeline.ArityError
import com.azure.feathr.pipeline.ColumnType
import com.azure.feathr.pipeline.Value
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
import kotlin.math.*
import kotlin.reflect.jvm.internal.ReflectProperties.Val

interface Function {
    fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
        return ColumnType.DYNAMIC
    }

    fun call(arguments: List<Value>): Value

    class NullaryFunctionWrapper<out R : Any?>(
        val function: java.util.function.Supplier<out R>,
        private val retType: ColumnType
    ) : Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return retType
        }

        override fun call(arguments: List<Value>): Value {
            return Value(this.function.get())
        }
    }

    class UnaryFunctionWrapper<in T : Any?, out R : Any?>(
        val function: java.util.function.Function<in T, out R>,
        private val retType: ColumnType
    ) :
        Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return retType
        }

        @Suppress("UNCHECKED_CAST")
        override fun call(arguments: List<Value>): Value {
            if (arguments.size != 1) throw ArityError("Required 1 arguments, but ${arguments.size} provided")
            return Value(this.function.apply(arguments[0].value as T))
        }
    }

    class UnaryGenericFunctionWrapper(val function: java.util.function.Function<Value, Value>) :
        Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return ColumnType.DYNAMIC
        }

        override fun call(arguments: List<Value>): Value {
            if (arguments.size != 1) throw ArityError("Required 1 arguments, but ${arguments.size} provided")
            return Value(this.function.apply(arguments[0]))
        }
    }

    class BinaryFunctionWrapper<in T1 : Any?, in T2 : Any?, out R : Any?>(
        val function: java.util.function.BiFunction<in T1, in T2, out R>,
        private val retType: ColumnType
    ) :
        Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return retType
        }

        @Suppress("UNCHECKED_CAST")
        override fun call(arguments: List<Value>): Value {
            if (arguments.size != 2) throw ArityError("Required 2 arguments, but ${arguments.size} provided")
            return Value(this.function.apply(arguments[0].value as T1, arguments[1].value as T2))
        }
    }

    class BinaryGenericFunctionWrapper(val function: java.util.function.BiFunction<Value, Value, Value>) :
        Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return ColumnType.DYNAMIC
        }

        override fun call(arguments: List<Value>): Value {
            if (arguments.size != 2) throw ArityError("Required 2 arguments, but ${arguments.size} provided")
            return Value(this.function.apply(arguments[0], arguments[1]))
        }
    }

    interface Function3<T1, T2, T3, R> {
        fun apply(t1: T1, t2: T2, t3: T3): R
    }

    class TernaryFunctionWrapper<in T1 : Any?, in T2 : Any?, in T3 : Any?, out R : Any?>(
        val function: Function3<in T1, in T2, in T3, out R>,
        private val retType: ColumnType
    ) :
        Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return retType
        }

        @Suppress("UNCHECKED_CAST")
        override fun call(arguments: List<Value>): Value {
            if (arguments.size != 3) throw ArityError("Required 3 arguments, but ${arguments.size} provided")
            return Value(
                this.function.apply(
                    arguments[0].value as T1,
                    arguments[1].value as T2,
                    arguments[2].value as T3
                )
            )
        }
    }

    class TernaryGenericFunctionWrapper(val function: Function3<Value, Value, Value, Value>) :
        Function {
        override fun getResultType(argumentTypes: List<ColumnType>): ColumnType {
            return ColumnType.DYNAMIC
        }

        override fun call(arguments: List<Value>): Value {
            if (arguments.size != 2) throw ArityError("Required 3 arguments, but ${arguments.size} provided")
            return Value(this.function.apply(arguments[0], arguments[1], arguments[1]))
        }
    }

    companion object {
        val UTC: ZoneId = ZoneId.of("UTC")
        val functions: MutableMap<String, Function> = mutableMapOf()

        init {
            // SparkSQL builtin functions
            register("abs", AbsFunction())
            register("acos", Math::acos)
            register("acosh", ::acosh)
            register("add_months", OffsetDateTime::plusMonths)
//            aes_decrypt
//            aes_encrypt
//            aggregate
//            and
//            any
//            approx_count_distinct
//            approx_percentile
            register("array", MakeArray())
//            array_agg
            register("array_contains", binaryg { x, y -> Value(x.getArray().contains(y.value)) })
            register("array_distinct", unaryg { x -> Value(x.getArray().distinct()) })
//            array_except
//            array_intersect
//            array_join
//            array_max
//            array_min
//            array_position
//            array_remove
//            array_repeat
//            array_size
//            array_sort
//            array_union
//            arrays_overlap
//            arrays_zip
//            ascii
            register("asin", ::asin)
            register("asinh", ::asinh)
//            assert_true
            register("atan", ::atan)
            register("atan2", ::atan2)
            register("atanh", ::atanh)
//            avg
//            base64
//            between
            register("bigint", TypeConvertor(ColumnType.LONG))
//            bin
//            binary
//            bit_and
//            bit_count
//            bit_get
//            bit_length
//            bit_or
//            bit_xor
//            bool_and
//            bool_or
            register("boolean", TypeConvertor(ColumnType.BOOL))
//            bround
            register("btrim") { s: String -> s.trim() }
//            cardinality
//            case
//            cast
            register("cbrt", Math::cbrt)
            register("ceil", ::ceil)
            register("ceiling", ::ceil)
//            char
            register("char_length", String::length)
            register("character_length", String::length)
//            chr
//            coalesce
//            collect_list
//            collect_set
//            concat
//            concat_ws
//            contains
//            conv
//            corr
            register("cos", ::cos)
            register("cosh", ::cosh)
            register("cot") { x: Double -> 1.0 / tan(x) }
//            count
//            count_if
//            count_min_sketch
//            covar_pop
//            covar_samp
//            crc32
            register("csc") { x: Double -> 1.0 / sin(x) }
//            cume_dist
//            current_catalog
//            current_database
//            current_date
            register("current_date", nullary(OffsetDateTime::now))
//            current_timestamp
            register("current_timezone", nullary { "UTC" })  // Always use UTC
//            current_user
            register("date", TypeConvertor(ColumnType.DATETIME))
//            date_add
//            date_format
//            date_from_unix_date
//            date_part
//            date_sub
//            date_trunc
//            datediff
            register("day", OffsetDateTime::getDayOfMonth)
            register("dayofmonth", OffsetDateTime::getDayOfMonth)
            register("dayofweek", OffsetDateTime::getDayOfWeek)
            register("dayofyear", OffsetDateTime::getDayOfYear)
//            decimal
//            decode
            register("degrees") { x: Double -> x * 180.0 / PI }
//            dense_rank
//            div
            register("double", TypeConvertor(ColumnType.DOUBLE))
            register("e", nullary { return@nullary Math::E })
//            element_at
//            elt
//            encode
            register("endswith") { x: String, y: String -> x.endsWith(y) }
//            every
//            exists
            register("exp", unary(Math::exp))
//            explode
//            explode_outer
            register("expm1", ::expm1)
//            extract
//            factorial
//            filter
//            find_in_set
//            first
//            first_value
//            flatten
            register("float", TypeConvertor(ColumnType.FLOAT))
            register("floor", ::floor)
//            forall
//            format_number
//            format_string
//            from_csv
//            from_json
//            from_unixtime
//            from_utc_timestamp
//            get_json_object
//            getbit
//            greatest
//            grouping
//            grouping_id
//            hash
//            hex
//            histogram_numeric
            register("hour") { x: OffsetDateTime -> x.hour }
//            hypot
//            if
            register("ifnull", binaryg { x: Value, y: Value -> if (x.isNull()) y else x })
//            ilike
//            in
//            initcap
//            inline
//            inline_outer
//            input_file_block_length
//            input_file_block_start
//            input_file_name
//            instr
            register("int", TypeConvertor(ColumnType.INT))
            register("isnan") { x: Double -> x.isNaN() }
            register("isnotnull", unaryg { Value(!it.isNull()) })
            register("isnull", unaryg { Value(it.isNull()) })
//            java_method
//            json_array_length
//            json_object_keys
//            json_tuple
//            kurtosis
//            lag
//            last
//            last_day
//            last_value
//            lcase
//            lead
//            least
//            left
//            length
//            levenshtein
//            like
            register("ln", ::ln)
//            locate
            register("log", ::log)
            register("log10", ::log10)
            register("log1p", ::ln1p)
            register("log2", ::log2)
            register("lower") { x: String -> x.lowercase() }
//            lpad
            register("ltrim") { x: String -> x.trimStart() }
//            make_date
//            make_dt_interval
//            make_interval
//            make_timestamp
//            make_ym_interval
//            map
//            map_concat
//            map_contains_key
//            map_entries
//            map_filter
//            map_from_arrays
//            map_from_entries
//            map_keys
//            map_values
//            map_zip_with
//            max
//            max_by
//            md5
//            mean
//            min
//            min_by
            register("minute") { x: OffsetDateTime -> x.minute }
//            mod
//            monotonically_increasing_id
            register("month") { x: OffsetDateTime -> x.month }
//            months_between
//            named_struct
            register("nanvl") { x: Double, y: Double -> if (x.isNaN()) y else x }
            register("negative") { x: Double -> -x }
            register("next_day") { x: OffsetDateTime -> x.plusDays(1) }
//            not
            register("now", nullary(OffsetDateTime::now))
//            nth_value
//            ntile
            register("nullif", binaryg { x, y -> if (x == y) Value.NULL else x })
            register("nvl", binaryg { x, y -> if (x.isNull()) y else x })
//            nvl2
//            octet_length
//            or
//            overlay
//            parse_url
//            percent_rank
//            percentile
//            percentile_approx
            register("pi", nullary { return@nullary Math::PI })
//            pmod
//            posexplode
//            posexplode_outer
//            position
            register("positive") { x: Double -> x }
            register("pow", Math::pow)
            register("power", Math::pow)
//            printf
//            quarter
            register("radians") { x: Double -> x * PI / 180.0 }
//            raise_error
            register("rand", RandFunctions::rand)
//            randn
            register("random", RandFunctions::rand)
//            rank
//            reflect
//            regexp
//            regexp_extract
//            regexp_extract_all
//            regexp_like
//            regexp_replace
//            regr_avgx
//            regr_avgy
//            regr_count
//            regr_r2
            register("repeat") { s: String, n: Int -> s.repeat(n) }
//            replace
//            reverse
//            right
//            rint
//            rlike
//            round
            register("round", ::round)
//            row_number
//            rpad
            register("rtrim") { x: String -> x.trimEnd() }
//            schema_of_csv
//            schema_of_json
            register("sec") { x: Double -> 1 / cos(x) }
            register("second") { x: OffsetDateTime -> x.second }
//            sentences
//            sequence
//            session_window
//            sha
//            sha1
//            sha2
//            shiftleft
//            shiftright
//            shiftrightunsigned
            register("shuffle", RandFunctions::shuffle)
//            sign
//            signum
            register("sin", ::sin)
            register("sinh", ::sinh)
//            size
//            skewness
//            slice
//            smallint
//            some
//            sort_array
//            soundex
            register("space") { n: Int -> " ".repeat(n) }
//            spark_partition_id
//            split
//            split_part
            register("sqrt", ::sqrt)
//            stack
            register("startswith") { x: String, y: String -> x.startsWith(y) }
//            std
//            stddev
//            stddev_pop
//            stddev_samp
//            str_to_map
            register("string", TypeConvertor(ColumnType.STRING))
//            struct
//            substr
//            substring
//            substring_index
//            sum
            register("tan", ::tan)
            register("tanh", ::tanh)
            register("timestamp", TypeConvertor(ColumnType.DATETIME))
            register("timestamp_micros") { x: Long ->
                LocalDateTime.ofEpochSecond(
                    x / 1000_1000,
                    (x % 1000_000).toInt() * 1000,
                    ZoneOffset.UTC
                ).atOffset(ZoneOffset.UTC)
            }
            register("timestamp_millis") { x: Long ->
                LocalDateTime.ofEpochSecond(
                    x / 1000,
                    (x % 1000).toInt() * 1000_000,
                    ZoneOffset.UTC
                ).atOffset(ZoneOffset.UTC)
            }
            register("timestamp_seconds") { x: Long ->
                LocalDateTime.ofEpochSecond(x, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)
            }
//            tinyint
//            to_binary
//            to_csv
//            to_date
//            to_json
//            to_number
//            to_timestamp
            register("to_unix_timestamp") { x: OffsetDateTime ->
                // TODO: format
                x.toLocalDateTime().toEpochSecond(ZoneOffset.UTC)
            }
            register("to_utc_timestamp", ToUtcTimeStamp())
//            transform
//            transform_keys
//            transform_values
//            translate
            register("trim") { x: String -> x.trim() }
//            trunc
//            try_add
//            try_avg
//            try_divide
//            try_element_at
//            try_multiply
//            try_subtract
//            try_sum
//            try_to_binary
//            try_to_number
//            typeof
            register("ucase") { x: String -> x.uppercase() }
//            unbase64
//            unhex
//            unix_date
//            unix_micros
//            unix_millis
//            unix_seconds
            register("unix_timestamp") { x: OffsetDateTime ->
                // TODO: Default parameters and format
                x.toLocalDateTime().toEpochSecond(ZoneOffset.UTC)
            }
            register("upper") { x: String -> x.uppercase() }
            register("uuid", nullary { UUID.randomUUID().toString() })
//            var_pop
//            var_samp
//            variance
//            version
            register("weekday") { x: OffsetDateTime -> x.dayOfWeek.value }
//            weekofyear
//            when
//            width_bucket
//            window
//            xpath
//            xpath_boolean
//            xpath_double
//            xpath_float
//            xpath_int
//            xpath_long
//            xpath_number
//            xpath_short
//            xpath_string
//            xxhash64
            register("year", OffsetDateTime::getYear)
//            zip_with

            // Type conversion
            register("to_long", TypeConvertor(ColumnType.LONG))
            register("to_float", TypeConvertor(ColumnType.FLOAT))
            register("to_double", TypeConvertor(ColumnType.DOUBLE))
            register("to_string", TypeConvertor(ColumnType.STRING))
            register("to_object", TypeConvertor(ColumnType.OBJECT))
            register("to_dynamic", TypeConvertor(ColumnType.DYNAMIC))

            // String ops
            register("substring", Substring())
            register("split", Split())

            // Array ops
            register("make_array", MakeArray())

            // Misc
            register("len", Len())
            register("case", Case())
            register("bucket", Bucket())
//            register("timestamp", Timestamp())
        }

        fun <T : Any> getType(cls: Class<T>): ColumnType {
            return when (cls) {
                Boolean::class.java -> ColumnType.BOOL
                Int::class.java -> ColumnType.INT
                Long::class.java -> ColumnType.LONG
                Float::class.java -> ColumnType.FLOAT
                Double::class.java -> ColumnType.DOUBLE
                String::class.java -> ColumnType.STRING
                List::class.java -> ColumnType.ARRAY
                Map::class.java -> ColumnType.OBJECT
                else -> ColumnType.ERROR
            }
        }

        /**
         * Register a Function with name
         */
        @JvmStatic
        fun register(name: String, function: Function) {
            functions[name] = function
        }

        /**
         * Wrap a nullary function, which takes no parameter and returns `R`
         */
        @JvmStatic
        fun <R : Any?> nullary(function: java.util.function.Supplier<out R>, retType: ColumnType): Function {
            return NullaryFunctionWrapper(function, retType)
        }

        /**
         * Wrap a unary function, which takes 1 parameter in `T` type and returns `R`
         */
        @JvmStatic
        fun <T : Any?, R : Any?> unary(
            function: java.util.function.Function<in T, out R>,
            retType: ColumnType
        ): Function {
            return UnaryFunctionWrapper(function, retType)
        }

        /**
         * Wrap a unary generic function, which takes 1 parameter in `Value` type and returns `Value`
         */
        @JvmStatic
        fun unaryg(function: java.util.function.Function<Value, Value>): Function {
            return UnaryGenericFunctionWrapper(function)
        }

        /**
         * Wrap a binary generic function, which takes 2 parameters in `T1` and `T2` type and returns `R`
         */
        @JvmStatic
        fun <T1 : Any?, T2 : Any?, R : Any?> binary(
            function: java.util.function.BiFunction<in T1, in T2, out R>,
            retType: ColumnType
        ): Function {
            return BinaryFunctionWrapper(function, retType)
        }

        /**
         * Wrap a binary generic function, which takes 2 parameters in `Value` type and returns `Value`
         */
        @JvmStatic
        fun binaryg(function: java.util.function.BiFunction<Value, Value, Value>): Function {
            return BinaryGenericFunctionWrapper(function)
        }

        /**
         * Wrap a binary generic function, which takes 2 parameters in `T1` and `T2` type and returns `R`
         */
        @JvmStatic
        fun <T1 : Any?, T2 : Any?, T3 : Any?, R : Any?> ternary(
            function: Function3<in T1, in T2, in T3, out R>,
            retType: ColumnType
        ): Function {
            return TernaryFunctionWrapper(function, retType)
        }

        /**
         * Wrap a binary generic function, which takes 2 parameters in `Value` type and returns `Value`
         */
        @JvmStatic
        fun ternaryg(function: Function3<Value, Value, Value, Value>): Function {
            return TernaryGenericFunctionWrapper(function)
        }

        inline fun <reified R : Any> register(name: String, noinline f: () -> R) {
            register(name, nullary(f))
        }

        inline fun <reified T : Any, reified R : Any> register(name: String, noinline f: (T) -> R) {
            register(name, unary(f))
        }

        inline fun <reified T1 : Any, reified T2 : Any, reified R : Any> register(
            name: String,
            noinline f: (T1, T2) -> R
        ) {
            register(name, binary(f))
        }

        inline fun <reified R : Any> nullary(function: java.util.function.Supplier<out R>): Function {
            return NullaryFunctionWrapper(function, getType(R::class.java))
        }

        inline fun <reified T : Any?, reified R : Any> unary(function: java.util.function.Function<in T, out R>): Function {
            return UnaryFunctionWrapper(function, getType(R::class.java))
        }

        inline fun <reified T1 : Any?, reified T2 : Any?, reified R : Any> binary(function: java.util.function.BiFunction<in T1, in T2, out R>): Function {
            return BinaryFunctionWrapper(function, getType(R::class.java))
        }

        inline fun <reified T1 : Any?, reified T2 : Any?, reified T3 : Any?, reified R : Any> ternary(function: Function3<in T1, in T2, in T3, out R>): Function {
            return TernaryFunctionWrapper(function, getType(R::class.java))
        }
    }
}

