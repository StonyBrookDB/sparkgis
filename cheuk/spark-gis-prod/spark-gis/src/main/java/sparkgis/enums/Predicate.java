package sparkgis.enums;

public enum Predicate{
    INTERSECTS ("st_intersects"),
    TOUCHES ("st_touches"),
    CROSSES ("st_crosses"),
    WITHIN ("st_within"),
    CONTAINS ("st_contains"),
    OVERLAP ("st_overlap");
    
    public final String value;
    
    private Predicate(String value){
	this.value = value;
    }
}
