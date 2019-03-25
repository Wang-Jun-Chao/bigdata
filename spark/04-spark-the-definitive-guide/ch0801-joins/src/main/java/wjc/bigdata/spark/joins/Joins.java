package wjc.bigdata.spark.joins;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple4;
import scala.collection.Seq;
import scala.reflect.ClassManifestFactory;
import wjc.bigdata.spark.util.SparkUtils;


/**
 * TODO 有问题运行不了
 * @author: wangjunchao(王俊超)
 * @time: 2019-03-24 21:15
 **/
public class Joins {
    private final static Logger logger = LoggerFactory.getLogger(Joins.class);

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ch0701-aggregations")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        Seq personSeq = SparkUtils.seq(
                new GenericRow(new Object[]{0, "Bill Chambers", 0, new int[]{100}}),
                new GenericRow(new Object[]{1, "Matei Zaharia", 1, new int[]{500, 250, 100}}),
                new GenericRow(new Object[]{2, "Michael Armbrust", 1, new int[]{250, 100}}));

        RDD personRDD = spark.sparkContext().parallelize(
                personSeq,
                spark.sparkContext().defaultParallelism(),
                ClassManifestFactory.classType(Tuple4.class));

        StructType parseSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("graduate_program", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("spark_status", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty()),
        });

        Dataset<Row> person = spark.createDataFrame(personRDD, parseSchema);
        person.printSchema();
        person.createOrReplaceTempView("person");

        Seq graduateProgramSeq = SparkUtils.seq(
                new GenericRow(new Object[]{0, "Masters", "School of Information", "UC Berkeley"}),
                new GenericRow(new Object[]{2, "Masters", "EECS", "UC Berkeley"}),
                new GenericRow(new Object[]{1, "Ph.D.", "EECS", "UC Berkeley"}));

        RDD graduateProgramRDD = spark.sparkContext().parallelize(
                graduateProgramSeq,
                spark.sparkContext().defaultParallelism(),
                ClassManifestFactory.classType(Tuple4.class));

        StructType graduateProgramSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("degree", DataTypes.StringType, true, Metadata.empty()),
                new StructField("department", DataTypes.StringType, true, Metadata.empty()),
                new StructField("school", DataTypes.StringType, true, Metadata.empty()),
        });

        Dataset<Row> graduateProgram = spark.createDataFrame(graduateProgramRDD, graduateProgramSchema);
        graduateProgram.printSchema();
        graduateProgram.createOrReplaceTempView("graduateProgram");

        Seq sparkStatusSeq = SparkUtils.seq(
                new GenericRow(new Object[]{500, "Vice President"}),
                new GenericRow(new Object[]{250, "PMC Member"}),
                new GenericRow(new Object[]{100, "Contributor"}));

        RDD sparkStatusRDD = spark.sparkContext().parallelize(
                sparkStatusSeq,
                spark.sparkContext().defaultParallelism(),
                ClassManifestFactory.classType(Tuple4.class));

        StructType sparkStatusSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("status", DataTypes.StringType, true, Metadata.empty()),
        });

        Dataset<Row> sparkStatus = spark.createDataFrame(sparkStatusRDD, sparkStatusSchema);
        sparkStatus.printSchema();
        sparkStatus.createOrReplaceTempView("sparkStatus");

        Column joinExpression = person.col("graduate_program").$eq$eq$eq(graduateProgram.col("id"));
        Column wrongJoinExpression = person.col("name").$eq$eq$eq(graduateProgram.col("school"));

        person.join(graduateProgram, joinExpression).show();
        String joinType = "inner";
        person.join(graduateProgram, joinExpression, joinType).show();

        joinType = "outer";
        person.join(graduateProgram, joinExpression, joinType).show();

        joinType = "left_outer";
        graduateProgram.join(person, joinExpression, joinType).show();

        joinType = "right_outer";
        person.join(graduateProgram, joinExpression, joinType).show();

        joinType = "left_semi";
        graduateProgram.join(person, joinExpression, joinType).show();


        Seq graduateProgramSeq2 = SparkUtils.seq(
                new GenericRow(new Object[]{0, "Masters", "Duplicated Row", "Duplicated School"}));

        RDD graduateProgramRDD2 = spark.sparkContext().parallelize(
                graduateProgramSeq2,
                spark.sparkContext().defaultParallelism(),
                ClassManifestFactory.classType(Tuple4.class));

        StructType graduateProgramSchema2 = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("degree", DataTypes.StringType, true, Metadata.empty()),
                new StructField("department", DataTypes.StringType, true, Metadata.empty()),
                new StructField("school", DataTypes.StringType, true, Metadata.empty()),
        });

        Dataset<Row> graduateProgram2 = spark.createDataFrame(graduateProgramRDD2, graduateProgramSchema2);
        Dataset<Row> gradProgram2 = graduateProgram.union(graduateProgram2);
        gradProgram2.createOrReplaceTempView("gradProgram2");
        gradProgram2.join(person, joinExpression, joinType).show();

        joinType = "left_anti";
        graduateProgram.join(person, joinExpression, joinType).show();

        joinType = "cross";
        graduateProgram.join(person, joinExpression, joinType).show();

        person.crossJoin(graduateProgram).show();

        person.withColumnRenamed("id", "personId")
                .join(sparkStatus, functions.expr("array_contains(spark_status, id)"))
                .show();

        Dataset<Row> gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program");
        Column joinExpr = gradProgramDupe.col("graduate_program")
                .$eq$eq$eq(person.col("graduate_program"));
        person.join(gradProgramDupe, joinExpr)
                .show();
        person.join(gradProgramDupe, joinExpr)
                .select("graduate_program")
                .show();

        person.join(gradProgramDupe, "graduate_program")
                .select("graduate_program")
                .show();

        person.join(gradProgramDupe, joinExpr)
                .drop(person.col("graduate_program"))
                .select("graduate_program")
                .show();

        joinExpr = person.col("graduate_program")
                .$eq$eq$eq(graduateProgram.col("id"));
        person.join(graduateProgram, joinExpr)
                .drop(graduateProgram.col("id"))
                .show();

        Dataset<Row> gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id");
        joinExpr = person.col("graduate_program")
                .$eq$eq$eq(gradProgram3.col("grad_id"));
        person.join(gradProgram3, joinExpr).show();

        joinExpr = person.col("graduate_program")
                .$eq$eq$eq(graduateProgram.col("id"));
        person.join(graduateProgram, joinExpr).explain();

        joinExpr = person.col("graduate_program").$eq$eq$eq(graduateProgram.col("id"));
        person.join(functions.broadcast(graduateProgram), joinExpr).explain();
    }
}
