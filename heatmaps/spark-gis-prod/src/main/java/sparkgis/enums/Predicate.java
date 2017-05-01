package sparkgis.enums;
/**
 * Predicate values corresponds to values defined in 
 * src/main/java/jni/native/gis.hpp
 */
public enum Predicate{
    // INTERSECTS ("st_intersects"),
    // TOUCHES ("st_touches"),
    // CROSSES ("st_crosses"),
    // WITHIN ("st_within"),
    // CONTAINS ("st_contains"),
    // OVERLAP ("st_overlap");

    INTERSECTS (1),
    TOUCHES (2),
    CROSSES (3),
    CONTAINS (4),
    ADJACENT (5),
    DISJOINT (6),
    EQUALS (7),
    DWITHIN (8),
    WITHIN (9),
    OVERLAPS (10),
    NEAREST (11),
    NEAREST_2 (12);
    
    public final int value;
    
    private Predicate(int value){
	this.value = value;
    }
}
