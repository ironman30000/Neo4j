import org.logicng.formulas.Formula;
import org.logicng.formulas.FormulaFactory;
import org.logicng.io.parsers.ParserException;
import org.logicng.io.parsers.PropositionalParser;
import org.logicng.transformations.simplification.*;

public class logic {
    public static void main (String[]args) throws ParserException {
        System.out.println("LOGIC NG");
        String[] string={"B|~B","A&~A","(A&B)|(A&C)","(A|B)|(C|A)","(A&B)&(A&C)","A&(A|B)|(C|D)"};
        for(String temp : string)
        {
            final FormulaFactory f= new FormulaFactory();
            final PropositionalParser p = new PropositionalParser(f);
            final Formula formula = p.parse(temp);
            final Formula nnf = formula.nnf();
            final Formula cnf = formula.cnf();

            String ff = formula.toString();
            String ff1 =nnf.toString();
            String ff2= cnf.toString();
            System.out.print(temp  + "                " + ff + "           "+ ff1 + "            " + ff2 + "   ");


            System.out.println();
        }

    }

}
