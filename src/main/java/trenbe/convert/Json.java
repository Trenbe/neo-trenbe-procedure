package trenbe.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import trenbe.result.MapResult;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class Json
{
    private static final Map<String,String> FULL_TEXT =
            stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    
    @UserFunction
    @Description("apoc.convert.toMap(value) | tries it's best to convert the value to a map")
    public Map<String, Object> toMap(@Name("map") Object map) {

        if (map instanceof PropertyContainer) {
            return ((PropertyContainer)map).getAllProperties();
        } else if (map instanceof Map) {
            return (Map<String, Object>) map;
        } else {
            return null;
        }
    }

    @Procedure("trenbe.convert.toTree")
    @Description("trenbe.convert.toTree([paths],[typeName]) creates a stream of nested documents representing the at least one root of these paths")
    public Stream<MapResult> toTree(@Name("paths") List<Path> paths, @Name(value = "typeName") String typeName) {
        if (paths.isEmpty()) return Stream.of(new MapResult(Collections.emptyMap()));

        Map<Long, Map<String, Object>> maps = new HashMap<>(paths.size() * 100);
        for (Path path : paths) {
            Iterator<PropertyContainer> it = path.iterator();
            while (it.hasNext()) {
                Node n = (Node) it.next();
                    Map<String, Object> nMap = maps.computeIfAbsent(n.getId(), (id) -> toMap(n));
                if (it.hasNext()) {
                    Relationship r = (Relationship) it.next();
                    Node m = r.getOtherNode(n);
                    Map<String, Object> mMap = maps.computeIfAbsent(m.getId(), (id) -> toMap(m));
                    typeName = !"".equals(typeName) ? typeName : r.getType().name().toLowerCase();
                    mMap = addRelProperties(mMap, typeName, r);
                    
                    if (!nMap.containsKey(typeName)) nMap.put(typeName, new ArrayList<>(16));
                    List list = (List) nMap.get(typeName);
                    if (!list.contains(mMap))
                        list.add(mMap); // todo performance, use set instead and convert to map at the end?
                }
            }
        }
        return paths.stream()
                .map(Path::startNode)
                .distinct()
                .map(n -> maps.remove(n.getId()))
                .map(m -> m == null ? Collections.<String,Object>emptyMap() : m)
                .map(MapResult::new);
    }

    private Map<String, Object> addRelProperties(Map<String, Object> mMap, String typeName, Relationship r) {
        Map<String, Object> rProps = r.getAllProperties();
        if (rProps.isEmpty()) return mMap;
        String prefix = typeName + ".";
        rProps.forEach((k, v) -> mMap.put(prefix + k, v));
        return mMap;
    }
   
}
