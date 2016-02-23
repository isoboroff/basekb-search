package gov.nist.basekb;

import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.Iterator;
import java.util.Collection;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import com.google.common.collect.Multimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ArrayListMultimap;

/**
   The basic Undeterred prints entities out using the basic output formats from Forecastles
*/
public class EntityRenderer {

	public static final String FIELD_NAME_SUBJECT = "subject";
    public static final String FIELD_NAME_TEXT    = "text";

	public EntityRenderer() {	}

    public String getSubjectName(Document subjectDoc) throws IOException {
        // Return the string name of `subjectDoc'.
        return subjectDoc.get(FIELD_NAME_SUBJECT);
    }

	public String normalizeNewlines(String value) {
		return value.replace('\n', ' ');
    }

	public LinkedListMultimap docToMultimap(Document d) {
	    LinkedListMultimap<String, String> m = LinkedListMultimap.create();
		for (IndexableField f : d.getFields()) {
			m.put(f.name(), f.stringValue());
		}
		return m;
	}

	protected Pattern wrapper = Pattern.compile("^\"(.*)\"@[a-z]+$");
	public String trim(String field_val) {
		return wrapper.matcher(field_val).group();
	}

	public String linkify(String field_val) {
		if (field_val.startsWith("f_m.")) {
			return "<a href=\"/lookup/" + field_val + "\">" + field_val + "</a>";
		} else {
			return field_val;
		}
	}

	public void render(Document d, StringWriter buf, double score) throws IOException {
		PrintWriter out = new PrintWriter(buf);
		Multimap<String, String> dmap = docToMultimap(d);
		String subj_name = d.get("subject");
		out.print(subj_name + ":");
		if (score >= 0.0) {
			out.print(" [score=" + score + "]");
		}
		out.println("");
        for (String field : dmap.keySet()) {
            if (! FIELD_NAME_SUBJECT.equals(field)) {
			    Collection<String> vals_list = dmap.get(field);
				Iterator<String> vals = vals_list.iterator();
				if (vals_list.size() == 1) {
					out.println("    " + field + ": " + linkify(normalizeNewlines(vals.next())));
				} else if (vals_list.size() > 10) {
					out.println("    " + field + ":");
					for (int i = 0; i < 10; i++) {
						out.println("        " + linkify(normalizeNewlines(vals.next())));
					}
					out.println("        <a href=\"/lookup/" + subj_name + "#"
								+ field + "\">(and " + Integer.toString(vals_list.size()-10)
								+ " more...)</a>");
				} else {
					out.println("    " + field + ":");
					while (vals.hasNext()) {
						out.println("        " + linkify(normalizeNewlines(vals.next())));
					}
				}
			}
		}
    }

	public void render(Document d, StringWriter buf) throws IOException {
		render(d, buf, -1.0);
    }
}
