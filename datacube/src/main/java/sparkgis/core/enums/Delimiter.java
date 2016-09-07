package sparkgis.core.enums;

public enum Delimiter{
    TAB ("\t"),
    PIPE ("\\|"),
    COMMA (",");

    public final String value;

    private Delimiter(String v){
	this.value = v;
    }
}
