package gov.nist.basekb;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;

/**
   Straight text entities, with a handy A NAME before each field.
*/
public class EntityTypeRenderer extends EntityRenderer {

	public HashMap<String, EntityRenderer> type_map = new HashMap();
	public final EntityRenderer DEFAULT = new EntityRenderer();
	public FreebaseSearcher tools = null;
	
	public EntityTypeRenderer() {
		this(null);
	}

	public EntityTypeRenderer(FreebaseSearcher tools) {
		this.tools = tools;
		EntityRenderer everything = new EntityRenderer(tools);
		everything.addPriorityField("f_type.object.name");
		everything.addPriorityField("f_common.topic.description");
 		everything.addPriorityField("f_common.topic.alias");
		everything.setTypeMatch("f_");
		type_map.put("f_common.topic", everything);
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
			if (type.stringValue().equals("f_common.topic") ||
				type.stringValue().equals("f_location.location") ||
				type.stringValue().equals("f_organization.organization")||
				type.stringValue().equals("f_people.person")) {						
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
