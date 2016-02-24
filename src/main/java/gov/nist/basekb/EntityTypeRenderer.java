package gov.nist.basekb;

import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.Iterator;
import java.util.Collection;
import java.util.HashMap;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import com.google.common.collect.Multimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ArrayListMultimap;

/**
   Straight text entities, with a handy A NAME before each field.
*/
public class EntityTypeRenderer extends EntityRenderer {

	public HashMap<String, EntityRenderer> type_map = new HashMap();
	public final EntityRenderer DEFAULT = new EntityRenderer();
	public FreebaseTools tools = null;
	
	public EntityTypeRenderer() {
		this(null);
	}

	public EntityTypeRenderer(FreebaseTools fbt) {
		tools = fbt;
		EntityRenderer person = new EntityRenderer(tools);
		person.addPriorityField("f_type.object.name");
		person.addPriorityField("f_common.topic.description");
		person.setTypeMatch("f_people.person");
		type_map.put("f_people.person", person);

		EntityRenderer place = new EntityRenderer(tools);
		place.addPriorityField("f_type.object.name");
		place.addPriorityField("f_common.topic.description");
		place.addPriorityField("f_base.locations");
		place.addPriorityField("f_government.political_district");
		place.addPriorityField("f_organization.organization_member");
		place.setTypeMatch("f_location");
		type_map.put("f_location.location", place);
	}
	
	public EntityRenderer getRendererByType(String type) {
		if (type == null) {
			return DEFAULT;
		} else if (type_map.containsKey(type)) {
			return type_map.get(type);
		} else {
			return DEFAULT;
		}
	}

	public String getBestType(Document d) {
		String best = null;
		for (IndexableField type : d.getFields("r_type")) {
			if (type.stringValue().equals("f_people.person") ||
				type.stringValue().equals("f_location.location") ||
				type.stringValue().equals("f_organization.organization")) {
				best = type.stringValue();
				break;
			}
		}
		return best;
	}
	
	public void render(Document d, StringWriter buf, double score) throws IOException {
		String t = getBestType(d);
		EntityRenderer r = getRendererByType(t);
		r.render(d, buf, score);
    }
}
