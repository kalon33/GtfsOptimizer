package com.transitfeeds.gtfssqloptimizer;

public class LatLng {

    public double latitude, longitude;
    public double latitudeE6, longitudeE6;
    
    public LatLng(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
        
        this.latitudeE6 = latitude * 1e6;
        this.longitudeE6 = longitude * 1e6;
    }

}
