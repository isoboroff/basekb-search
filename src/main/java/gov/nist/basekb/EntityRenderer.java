package gov.nist.basekb;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
   The basic Undeterred prints entities out using the basic output formats from Forecastles
*/
public class EntityRenderer {

	public static final String FIELD_NAME_SUBJECT = "subject";
    public static final String FIELD_NAME_TEXT    = "text";
	public LinkedHashSet<String> field_priority = new LinkedHashSet();
	public String type_match = null;
	public FreebaseSearcher tools = null;
	
	
	public EntityRenderer() {	}

	public EntityRenderer(FreebaseSearcher fbt) {
		tools = fbt;
	}

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

	public String linkify(String field_val) throws IOException {
		if (field_val.startsWith("f_m.")) {
			return "<a href=\"lookup/" + field_val + "\">" + expand(field_val) + "</a>";
		} else {
			return field_val;
		}
	}

	Pattern en_pattern = Pattern.compile("^\"(.*)\"@en$");
	public String expand(String field_val) throws IOException {
		String retval = field_val;
		if (tools != null) {
			int docid = tools.getSubjectDocID(field_val);
			if (docid >= 0) {
				Document doc = tools.getDocumentInMode(docid);
				for (IndexableField l : doc.getFields("rs_label")) {
					Matcher m = en_pattern.matcher(l.stringValue());
					if (m.matches())
						retval = field_val + " (" + m.group(1) + ")";
				}
			}
		}
		return retval;
	}
	
	public void addPriorityField(String field_name) {
		field_priority.add(field_name);
	}

	public void setTypeMatch(String type) {
		type_match = type;
	}
	
	public void render_field(String subject, String field, Collection<String> vals_coll, StringWriter buf) throws IOException {
		Iterator<String> vals = vals_coll.iterator();
		if (vals_coll.size() == 1) {
			buf.append("    " + field + ": " + linkify(normalizeNewlines(vals.next())) + "\n");
		} else if (vals_coll.size() > 10) {
			buf.append("    " + field + ":\n");
			for (int i = 0; i < 10; i++) {
				buf.append("        " + linkify(normalizeNewlines(vals.next())) + "\n");
			}
			buf.append("        <a href=\"lookup/" + subject + "#"
					   + field + "\">(and " + Integer.toString(vals_coll.size()-10)
					   + " more...)</a>\n");
		} else {
			buf.append("    " + field + ":\n");
			while (vals.hasNext()) {
				buf.append("        " + linkify(normalizeNewlines(vals.next())) + "\n");
			}
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
		for (String field : field_priority) {
			for (String v : dmap.get(field)) {
				out.println("    " + field + ": " + v);
			}
		}
		// Pass 1: type fields
		if (type_match != null) {
			for (String field : dmap.keySet()) {
				if (field.startsWith(type_match)) {
					render_field(subj_name, field, dmap.get(field), buf);
				}
			}
		}
		// Pass 2: remaining fields		
        for (String field : dmap.keySet()) {
            if (! (FIELD_NAME_SUBJECT.equals(field) ||
				   field_priority.contains(field) ||
				   (type_match != null && field.startsWith(type_match)))) {
				render_field(subj_name, field, dmap.get(field), buf);
			}
		}
    }

	public void render(Document d, StringWriter buf) throws IOException {
		render(d, buf, -1.0);
    }
}
