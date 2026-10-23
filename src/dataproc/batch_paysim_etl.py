"""ETL batch PaySim: Cloud Storage raw (CSV) → limpieza/enriquecimiento → curated (Parquet por fecha).

Dataproc Serverless:
  gcloud dataproc batches submit pyspark dataproc/batch_paysim_etl.py \
    --region us-central1 --version 2.2 \
    --service-account sa-lumina-pipelines@$PROJECT_ID.iam.gserviceaccount.com \
    --deps-bucket gs://$PROJECT_ID-lumina-temp \
    -- --entrada gs://$PROJECT_ID-lumina-raw/paysim/ \
       --salida gs://$PROJECT_ID-lumina-curated/transacciones/
"""
import argparse

from pyspark.sql import SparkSession, functions as F, types as T

FECHA_BASE = "2026-07-01 00:00:00"  # step 1 = 2026-07-01 01:00 UTC
TIPOS_VALIDOS = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
ESQUEMA = T.StructType([
    T.StructField("step", T.IntegerType()),
    T.StructField("type", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("nameOrig", T.StringType()),
    T.StructField("oldbalanceOrg", T.DoubleType()),
    T.StructField("newbalanceOrig", T.DoubleType()),
    T.StructField("nameDest", T.StringType()),
    T.StructField("oldbalanceDest", T.DoubleType()),
    T.StructField("newbalanceDest", T.DoubleType()),
    T.StructField("isFraud", T.IntegerType()),
    T.StructField("isFlaggedFraud", T.IntegerType()),
])
DINERO = T.DecimalType(18, 2)


def bucket(col, salt: str, n: int):
    """Mismo hash que simulator/common.py y lumina_gold.fn_bucket."""
    return F.conv(F.substring(F.md5(F.concat(F.lit(salt), col)), 1, 8), 16, 10).cast("long") % n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", required=True)
    ap.add_argument("--salida", required=True)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("lumina-batch-paysim")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())

    crudo = spark.read.csv(args.entrada, header=True, schema=ESQUEMA)
    total = crudo.count()

    limpio = (crudo
              .dropna(subset=["step", "type", "amount", "nameOrig", "nameDest"])
              .filter(F.col("type").isin(TIPOS_VALIDOS) & (F.col("amount") >= 0))
              .dropDuplicates())

    cliente = F.col("nameOrig")
    distrito = bucket(cliente, "d", 8) + 1
    edad = bucket(cliente, "a", 63) + 18
    es_efectivo = F.col("type").isin("CASH_IN", "CASH_OUT")
    mayor = edad >= 60

    enriquecido = limpio.select(
        F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in (
            "step", "type", "amount", "nameOrig", "nameDest", "oldbalanceOrg", "newbalanceOrig")])).alias("event_id"),
        F.timestamp_seconds(F.unix_timestamp(F.lit(FECHA_BASE)) + F.col("step") * 3600).alias("event_ts"),
        F.col("step"),
        F.col("type").alias("tipo"),
        F.col("amount").cast(DINERO).alias("monto"),
        cliente.alias("cliente_id"),
        F.col("oldbalanceOrg").cast(DINERO).alias("saldo_ant_orig"),
        F.col("newbalanceOrig").cast(DINERO).alias("saldo_nuevo_orig"),
        F.col("nameDest").alias("destino_id"),
        F.col("oldbalanceDest").cast(DINERO).alias("saldo_ant_dest"),
        F.col("newbalanceDest").cast(DINERO).alias("saldo_nuevo_dest"),
        F.when(es_efectivo, F.when(mayor, "AGENCIA").otherwise("CAJERO"))
         .otherwise(F.when(mayor & (bucket(cliente, "c", 2) == 0), "AGENCIA").otherwise("APP")).alias("canal"),
        (bucket(cliente, "e", 12) + 1).alias("entidad_id"),
        distrito.alias("distrito_id"),
        (F.lit(-12.06) + F.lit(0.08) * (((distrito - 1) % 4) - 1.5)
         + (bucket(cliente, "la", 1000) / 1000 - 0.5) * 0.04).alias("lat"),
        (F.lit(-77.04) + F.lit(0.10) * (F.floor((distrito - 1) / 4) - 0.5)
         + (bucket(cliente, "lo", 1000) / 1000 - 0.5) * 0.04).alias("lon"),
        F.col("isFraud").alias("is_fraud_label"),
        F.col("isFlaggedFraud").alias("is_flagged_fraud"),
        # errores de cuadre de saldo: señal fuerte de fraude en PaySim
        (F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg")).cast(DINERO).alias("err_saldo_orig"),
        (F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest")).cast(DINERO).alias("err_saldo_dest"),
    ).withColumn("fecha", F.to_date("event_ts"))

    (enriquecido.repartition("fecha")
     .write.mode("overwrite").partitionBy("fecha").parquet(args.salida))

    salida = spark.read.parquet(args.salida).count()
    print(f"[calidad] filas leídas={total} escritas={salida} descartadas={total - salida}")
    spark.stop()


if __name__ == "__main__":
    main()
