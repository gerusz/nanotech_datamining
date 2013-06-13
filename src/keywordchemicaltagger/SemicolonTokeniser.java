package keywordchemicaltagger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import uk.ac.cam.ch.wwmm.chemicaltagger.ChemicalTaggerTokeniser;
import uk.ac.cam.ch.wwmm.oscar.document.Token;
import uk.ac.cam.ch.wwmm.oscar.types.BioTag;
import uk.ac.cam.ch.wwmm.oscar.types.BioType;

public class SemicolonTokeniser implements ChemicalTaggerTokeniser {
        
        private static Pattern tokenPattern = Pattern.compile(";\\s+?");
        
        /*****************************
         * Default constructor method.
         ***************************/
        public SemicolonTokeniser(){
                
        }

        /********************************************
         * Tokenises a String on white space.
         * @param  inputSentence (String)
         * @return List<Token>
         *****************************************/
        public List<Token> tokenise(String inputSentence){
                List<Token> tokens = new ArrayList<Token>();
                Matcher m = tokenPattern.matcher(inputSentence);
                int tokenIndex = 0;
                while (m.find()) {
                        int start = m.start();
                        int end = m.end();
                        String value = m.group();
                        Token t = new Token(value, start, end, null, new BioType(BioTag.O), null);
                        t.setIndex(tokenIndex++);
                        tokens.add(t);
                }
                return tokens;
        }

}