import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {
    static String s = "<http://gcm.wdcm.org/data/gcmAnnotation1/enzyme/1.5.1.17> <http://gcm.wdcm.org/ontology/gcmAnnotation/v1/substrate> \"H2O\" .";
    static Pattern regex = Pattern.compile("(?:<)(http://[^>]+/)([^/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(http://[^>]+/)([^/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\")(.+)(?:\") (?:\\.)");


    public static void main(String args[]) {
        System.out.println(isProperty(s));
        Matcher m = property_regex.matcher(s);
        System.out.println(get(m, 2));
//        System.out.println(m.group(1));
    }

    //#1 http type #2 id #2 name #3 value
    private static Pattern property_regex = Pattern.compile("(?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:<)(http:\\/\\/[^>]+\\/)([^\\/][-A-Za-z0-9._#$%^&*!@~]+)(?:>) (?:\")(.+)(?:\") (?:\\.)");

    public static boolean isProperty(String str) {
        return property_regex.matcher(str).find();
    }

    public static String get(Matcher m, int index) {
        if (m.find()) return m.group(index);
        else return "";
    }
}
