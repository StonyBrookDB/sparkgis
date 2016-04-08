package sparkgis.pia;

public enum HMType{
    JACCARD (3, "jaccard"),
    DICE (2, "dice"),
    TILEDICE (-1, "tile_dice");

    public final int value;
    private final String strValue;
    
    private HMType(int value, String str){
	this.value = value;
	this.strValue = str;
    }
    public String toString(){return this.strValue;}
}
