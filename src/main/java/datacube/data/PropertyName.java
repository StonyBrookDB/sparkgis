package datacube.data;

public enum PropertyName{
    // From 'image' document
    CASEID ("image.caseid"),
	// From 'features' document
	AREA ("Area"),
	ELONGATION ("Elongation"),
	ROUNDNESS ("Roundness"),
	PHYSICAL_SIZE ("PhysicalSize"),
	FLATNESS ("Flatness"),
	EQUIVALENT_SPHERICAL_RADUIS ("EquivalentSphericalRadius"),
	EQUIVALENT_SPHERICAL_PERIMETER ("EquivalentSphericalPerimeter"),
	EQUIVALENT_ELLIPSOID_DIAMETER0 ("EquivalentEllipsoidDiameter0"),
	EQUIVALENT_ELLIPSOID_DIAMETER1 ("EquivalentEllipsoidDiameter1"),
	PERIMETER ("Perimeter"),
	NUM_OF_PIXELS ("NumberOfPixels"),
	NUM_OF_PIXELS_ON_BORDER ("NumberOfPixelsOnBorder"),
	PRINCIPAL_MOMENTS0 ("PrincipalMoments0"),
	PRINCIPAL_MOMENTS1 ("PrincipalMoments1"),    
	FERET_DIAMETER ("FeretDiameter")
	;
    // Add as required ...

    public final String value;
	
    private PropertyName(String name){
	this.value = name;
    }
}

/*
  -"Perimeter" : 37.27199935913086,
  -"EquivalentSphericalPerimeter" : 25.06599998474121,
  -"EquivalentSphericalRadius" : 3.9893999099731445,
  -"Area" : 25.5,
  -"EquivalentEllipsoidDiameter0" : 3.9184999465942383,
  -"NumberOfPixelsOnBorder" : 18,
  -"Roundness" : 0.6725299954414368,
  -"EquivalentEllipsoidDiameter1" : 16.246999740600586,
  -"NumberOfPixels" : 50,
  -"PrincipalMoments0" : 1.0441999435424805,
  -"PhysicalSize" : 50,
  -"PrincipalMoments1" : 17.951000213623047,
  -"FeretDiameter" : 0,
  -"Elongation" : 4.146200180053711,
  -"Flatness" : 4.146200180053711,
*/
