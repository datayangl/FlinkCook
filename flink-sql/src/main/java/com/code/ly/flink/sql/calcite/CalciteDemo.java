package com.code.ly.flink.sql.calcite;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Properties;

public class CalciteDemo {
    public static void main(String[] args) throws SqlParseException {

        // 创建2张数据表
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("USERS", new AbstractTable() { //note: add a table
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                builder.add("AGE", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                return builder.build();
            }
        });

        rootSchema.add("JOBS", new AbstractTable() {
            public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();

                builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                builder.add("COMPANY", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                return builder.build();
            }
        });
        String sql = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age \n" +
                "from users u join jobs j on u.name=j.name\n" +
                "where u.age > 30 and j.id>10\n" +
                "order by user_id";


        //1 SQL 解析（SQL–>SqlNode）
        SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        SqlNode sqlNode = parser.parseStmt();

        //2 SqlNode 验证
        //note: get metadata and namespace
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                factory,
                new CalciteConnectionConfigImpl(new Properties()));

        //note: 校验（包括对表名，字段名，函数名，字段类型的校验。）
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader, factory);
        SqlNode validateSqlNode = validator.validate(sqlNode);

        final RexBuilder rexBuilder =  new RexBuilder(factory);
        // init the planner
        // 这里也可以注册 VolcanoPlanner，这一步 planner 并没有使用
        HepProgramBuilder builder = new HepProgramBuilder();
        RelOptPlanner planner = new HepPlanner(builder.build());
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        //RelOptPlanner planner = new VolcanoPlanner();
        //note: init cluster: An environment for related relational expressions during the optimization of a query.
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        //note: init SqlToRelConverter
        final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                // .withConfig(frameworkConfig.getSqlToRelConverterConfig())
                .withTrimUnusedFields(false)
                //.withConvertTableAccess(false)
                .build(); //note: config
        // 创建 SqlToRelConverter 实例，cluster、calciteCatalogReader、validator 都传进去了，SqlToRelConverter 会缓存这些对象
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new DogView(), validator, calciteCatalogReader, cluster, StandardConvertletTable.INSTANCE, config);
        // convert to RelNode
        RelRoot root = sqlToRelConverter.convertQuery(validateSqlNode, false, true);

        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));

        RelNode relNode = root.rel;

        System.out.println("finish");
    }

    //DogView 的实现
    private static class DogView implements RelOptTable.ViewExpander {
        public DogView() {
        }

        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                                  List<String> viewPath) {
            return null;
        }
    }
}
