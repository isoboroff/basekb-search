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
   Straight text entities, with a handy A NAME before each field.
*/
public class LongFormRenderer extends EntityRenderer {

	public static final String FIELD_NAME_SUBJECT = "subject";
    public static final String FIELD_NAME_TEXT    = "text";

	public LongFormRenderer() {	}

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
				out.print("<a name=\"" + field + "\">");
				if (vals_list.size() == 1) {
					out.println("    " + field + ": " + linkify(normalizeNewlines(vals.next())));
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
