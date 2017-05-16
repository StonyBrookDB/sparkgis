package sparkgis.enums;

public enum HMType{
    JACCARD (2, "jaccard"),
    DICE (1, "dice"),
    TILEDICE (-1, "tile_dice");

    public final int value;
    public final String strValue;
    
    private HMType(int value, String str){
	this.value = value;
	this.strValue = str;
    }
    public String toString(){return this.strValue;}
}
