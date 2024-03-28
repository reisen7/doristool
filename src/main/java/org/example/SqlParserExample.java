package org.example;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.List;

public class SqlParserExample {
    public static void main(String[] args) {

        Select select = SelectUtils.buildSelectFromTable(new Table("t_sys_user"));
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

        //Where 等于 =
        EqualsTo equalsTo = new EqualsTo(); // 等于表达式
        equalsTo.setLeftExpression(new Column("username"));
        equalsTo.setRightExpression(new StringValue("李四"));
        plainSelect.setWhere(equalsTo);//会被后面的setWhere()方法覆盖

        //Where 大于 > 类似=
        GreaterThan gt = new GreaterThan();
        //Where 小于 < 类似=
        MinorThan mt = new MinorThan();

        //Where like
        LikeExpression likeExpression = new LikeExpression();
        likeExpression.setLeftExpression(new Column("username"));
        likeExpression.setRightExpression(new StringValue("张%"));
        plainSelect.setWhere(likeExpression);//会被后面的setWhere()方法覆盖

        //Where AND 连接多个条件
        AndExpression andExpression = new AndExpression();
        andExpression.setLeftExpression(equalsTo);
        andExpression.setRightExpression(likeExpression);
        plainSelect.setWhere(andExpression);

        //Where BETWEEN
        Between between = new Between();
        between.setBetweenExpressionStart(new LongValue(18));
        between.setBetweenExpressionEnd(new LongValue(30));
        between.setLeftExpression(new Column("age"));

        //Where OR 连接多个条件
        OrExpression orExpression = new OrExpression();
        orExpression.setLeftExpression(andExpression);
        orExpression.setRightExpression(between);
        plainSelect.setWhere(orExpression);


//        System.out.println(plainSelect);

        // 获取表名
        Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse("SELECT * FROM tab1");
        } catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }
        Select selectStatement = (Select) stmt;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        System.out.println(tableList);


    }

    public static String getTableName (String sql){
        Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }
        Select selectStatement = (Select) stmt;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        System.out.println("=============================获取表名===============================");
        System.out.println("table name : "+ tableList);
        System.out.println("==================================================================");
        return tableList.get(0);
    }
}
