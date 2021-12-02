#include "arrow/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

extern "C"
{
#include "postgres.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
}

class BaseDataBuilder {
public:
    virtual ~BaseDataBuilder() {};
    virtual void Append(Datum) = 0;
    virtual void AppendNull() = 0;
    virtual void Finish(std::shared_ptr<arrow::Array>* out) = 0;
};

template <typename T>
class DataBuilder : public BaseDataBuilder
{
private:
    T builder;
public:
    ~DataBuilder<T>() {}
    void Append(Datum datum); /* specializations are implemented below */
    void AppendNull() {
        builder.AppendNull();
    }
    void Finish(std::shared_ptr<arrow::Array>* out) {
        builder.Finish(out);
    }
};

using BoolBuilder = DataBuilder<arrow::BooleanBuilder>;
using Int2Builder = DataBuilder<arrow::Int16Builder>;
using Int4Builder = DataBuilder<arrow::Int32Builder>;
using Int8Builder = DataBuilder<arrow::Int64Builder>;
using DateBuilder = DataBuilder<arrow::Date32Builder>;
using TextBuilder = DataBuilder<arrow::StringBuilder>;
using ByteaBuilder = DataBuilder<arrow::BinaryBuilder>;

template <>
void BoolBuilder::Append(Datum datum) {
    builder.Append(DatumGetBool(datum));
}

template <>
void Int2Builder::Append(Datum datum) {
    builder.Append(DatumGetInt16(datum));
}

template <>
void Int4Builder::Append(Datum datum) {
    builder.Append(DatumGetInt32(datum));
}

template <>
void Int8Builder::Append(Datum datum) {
    builder.Append(DatumGetInt64(datum));
}

template <>
void DateBuilder::Append(Datum datum) {
    builder.Append(DatumGetDateADT(datum)
        - (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
}

template <>
void TextBuilder::Append(Datum datum) {
    builder.Append(TextDatumGetCString(datum));
}

template <>
void ByteaBuilder::Append(Datum datum) {
    bytea *b = DatumGetByteaP(datum);

    builder.Append(VARDATA_ANY(b), VARSIZE_ANY_EXHDR(b));
}

class TimestampBuilder : public BaseDataBuilder
{
private:
    arrow::TimestampBuilder builder;
public:
    TimestampBuilder()
        : builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()) {}
    ~TimestampBuilder() {}
    void Append(Datum datum) {
        pg_time_t t = timestamptz_to_time_t(DatumGetTimestamp(datum));

        builder.Append(t * 1000000);
    }
    void AppendNull() {
        builder.AppendNull();
    }
    void Finish(std::shared_ptr<arrow::Array>* out) {
        builder.Finish(out);
    }
};

static BaseDataBuilder *
create_data_builder(Oid typoid)
{
    switch (typoid)
    {
        case BOOLOID:
            return new BoolBuilder();
        case INT2OID:
            return new Int2Builder();
        case INT4OID:
            return new Int4Builder();
        case INT8OID:
            return new Int8Builder();
        case DATEOID:
            return new DateBuilder();
        case TIMESTAMPOID:
            return new TimestampBuilder();
        case TEXTOID:
            return new TextBuilder();
        case BYTEAOID:
            return new ByteaBuilder();
        default:
            throw std::runtime_error("parquet_fdw: unknown type");
    }
}

std::shared_ptr<arrow::DataType>
pg_to_arrow_type(Oid typoid)
{
    switch (typoid)
    {
        case BOOLOID:
            return arrow::boolean();
        case INT2OID:
            return arrow::int16();
        case INT4OID:
            return arrow::int32();
        case INT8OID:
            return arrow::int64();
        case DATEOID:
            return arrow::date32();
        case TIMESTAMPOID:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case TEXTOID:
            return arrow::utf8();
        case BYTEAOID:
            return arrow::binary();
        default:
            elog(ERROR, "parquet_fdw: export data of type %i is not supported", typoid);
    }
}

extern "C"
{

PG_FUNCTION_INFO_V1(export_to_parquet);
Datum
export_to_parquet(PG_FUNCTION_ARGS)
{
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    std::shared_ptr<arrow::Schema>  schema;
    std::vector<BaseDataBuilder *>      array_builders;
    int         rows;
    TupleDesc   tupdesc;
    char       *query = TextDatumGetCString(PG_GETARG_TEXT_P(0));
    char       *filename = TextDatumGetCString(PG_GETARG_TEXT_P(1));
    bool        error = false;
    char        errmsg[256];
    int         rowgroup = 0;

    SPI_connect();
    SPIPlanPtr plan = SPI_prepare(query, 0, NULL);
    Portal cursor = SPI_cursor_open("export_parquet", plan, NULL, NULL, true);

    SPI_cursor_fetch(cursor, true, 100000);

    if (!SPI_tuptable)
        elog(ERROR, "parquet_fdw: SPI error");
    tupdesc = SPI_tuptable->tupdesc;

    try {
        /* Init schema */
        arrow::SchemaBuilder schema_builder;
        for (int i = 0; i < tupdesc->natts; ++i)
        {
            FormData_pg_attribute *attr = &tupdesc->attrs[i];
            std::string             name(attr->attname.data);
            Oid                     typoid = attr->atttypid;

            schema_builder.AddField(arrow::field(name, pg_to_arrow_type(typoid)));
            array_builders.push_back(create_data_builder(typoid));
        }

        PARQUET_ASSIGN_OR_THROW(
            schema,
            schema_builder.Finish());

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        PARQUET_ASSIGN_OR_THROW(
            outfile,
            arrow::io::FileOutputStream::Open(filename));

        parquet::WriterProperties::Builder builder;
        // builder.compression(parquet::Compression::GZIP);
        builder.compression(parquet::Compression::ZSTD);
        auto props = builder.build();

        parquet::arrow::FileWriter::Open(*schema, ::arrow::default_memory_pool(),
            outfile, props, parquet::default_arrow_writer_properties(),
            &writer);
    } catch (const std::exception &e) {
        error = true;
        strcpy(errmsg, e.what());
    }
    if (error)
        elog(ERROR, "parquet_fdw: %s", errmsg);

    while ((rows = SPI_processed) > 0)
    {
        if (SPI_tuptable) {
            SPITupleTable   *tuptable = SPI_tuptable;
            TupleDesc       tupdesc = SPI_tuptable->tupdesc;

            for (int i = 0; i < rows; ++i) {
                HeapTuple   tuple = tuptable->vals[i];

                for (int j = 0; j < tupdesc->natts; ++j)
                {
                    bool        isnull;
                    Datum       value;

                    value = SPI_getbinval(tuple, tupdesc, j + 1, &isnull);

                    /* TODO: try-catch */
                    if (!isnull)
                        array_builders[j]->Append(value);
                    else
                        array_builders[j]->AppendNull();
                }
            }

            try {
                std::vector<std::shared_ptr<arrow::Array>> columns;
                for (int j = 0; j < tupdesc->natts; ++j)
                {
                    std::shared_ptr<arrow::Array> arr;

                    array_builders[j]->Finish(&arr);
                    columns.push_back(arr);
                }

                std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, columns);

                writer->WriteTable(*table, rows);
            } catch (const std::exception &e) {
                error = true;
                strcpy(errmsg, e.what());
            }
            if (error)
                elog(ERROR, "parquet_fdw: %s", errmsg);
        }

        elog(LOG, "rowgroup %i done", rowgroup++);

        SPI_cursor_fetch(cursor, true, 100000);
    }
    writer->Close();

    /* free array_builders */
    for (BaseDataBuilder *d: array_builders)
        delete d;

    SPI_finish();

    PG_RETURN_VOID();
}

}
