package com.transitfeeds.gtfssqloptimizer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Optimizer {

    private Connection mConnection;
    private static final double SHAPE_REDUCE_TOLERANCE = 0.1;
    
    public Optimizer(Connection connection) {
        mConnection = connection;
    }

    public void run() throws SQLException {
        mConnection.setAutoCommit(false);
        
        optimizeShapes();
        buildPatterns();
        
        System.err.println("All done.");
        
        mConnection.commit();
        
        mConnection.setAutoCommit(true);
        
        runQuery("VACUUM");
        runQuery("ANALYZE");
    }

    private void runQuery(String query) throws SQLException {
        PreparedStatement stmt = mConnection.prepareStatement(query);
        stmt.executeUpdate();
        stmt.close();
    }

    private PreparedStatement mPatternInsertStmt;
    private PreparedStatement mTripUpdateStmt;
    private PreparedStatement mStopTimeStmt;
    
    private void buildPatterns() throws SQLException {
        System.err.println("Building patterns");
        
        runQuery("DROP TABLE IF EXISTS patterns");
        runQuery("CREATE TABLE patterns (pattern_index INTEGER, stop_index INTEGER, arrival_offset_secs INTEGER, departure_offset_secs INTEGER, stop_sequence INTEGER, last_stop INTEGER, shape_dist_traveled REAL, stop_headsign TEXT, pickup_type INTEGER, drop_off_type INTEGER)");
        runQuery("ALTER TABLE trips ADD pattern_index INTEGER");
        
        mPatternInsertStmt = mConnection.prepareStatement("INSERT INTO PATTERNS (pattern_index, stop_index, arrival_offset_secs, departure_offset_secs, stop_sequence, last_stop, stop_headsign, pickup_type, drop_off_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        mTripUpdateStmt = mConnection.prepareStatement("UPDATE TRIPS SET pattern_index = ? WHERE trip_index = ?");
        mStopTimeStmt = mConnection.prepareStatement("SELECT stop_index, arrival_time_secs, departure_time_secs, stop_headsign, pickup_type, drop_off_type FROM stop_times WHERE trip_index = ? ORDER BY stop_sequence");
        
        PreparedStatement stmt = mConnection.prepareStatement("SELECT trip_index, departure_time_secs FROM trips");
        
        ResultSet result = stmt.executeQuery();
        
        int patternIndex = 1;
        
        while (result.next()) {
            int tripIndex = result.getInt(1);
            int departureTimeSecs = result.getInt(2);
            
            if (patternIndex % 100 == 0) {
                System.err.println(" " + patternIndex);
            }
            else {
                System.err.print(".");
            }
            
            buildPatternForTrip(patternIndex, tripIndex, departureTimeSecs);
            patternIndex++;
        }
        
        System.err.println();
        
        result.close();
        stmt.close();
        
        runQuery("CREATE INDEX patterns_pattern_index ON patterns (pattern_index)");
        runQuery("CREATE INDEX patterns_stop_index ON patterns (stop_index)");
        runQuery("CREATE INDEX trips_pattern_index ON trips (pattern_index)");
        runQuery("DROP TABLE stop_times");
        
        mStopTimeStmt.close();
        mPatternInsertStmt.close();
        mTripUpdateStmt.close();
    }
    
    private Map<String, Integer> mPatterns = new HashMap<String, Integer>();
    
    private class PatternRow {
        int stopIndex;
        int arrivalOffset = -1;
        int departureOffset = -1;
        String headsign;
        int pickup = -1;
        int dropOff = -1;
        
        public String getHash() {
            return String.format("%d,%d,%d,%s,%d,%d", stopIndex, arrivalOffset, departureOffset, headsign, pickup, dropOff);
        }
    }
    
    private int buildPatternForTrip(int patternIndex, int tripIndex, int tripDepartureTimeSecs) throws SQLException {
        
        mStopTimeStmt.setInt(1, tripIndex);
        
        ResultSet result = mStopTimeStmt.executeQuery();
        
        List<String> rowHashes = new ArrayList<String>();
        List<PatternRow> rows = new ArrayList<Optimizer.PatternRow>();
        
        while (result.next()) {
            PatternRow row = new PatternRow();
            row.stopIndex = result.getInt(1);
            
            int arrivalSecs = result.getInt(2);
            
            if (!result.wasNull()) {
                row.arrivalOffset = arrivalSecs - tripDepartureTimeSecs;
            }
            
            int departureSecs = result.getInt(3);
            
            if (!result.wasNull()) {
                row.departureOffset = departureSecs - tripDepartureTimeSecs;
            }
            
            row.headsign = result.getString(4);
            row.pickup = result.getInt(5);
            row.dropOff = result.getInt(6);
            
            rowHashes.add(row.getHash());
            rows.add(row);
        }
        
        String tripHash = StringUtils.join(rowHashes, "|");
        
        Integer existingPatternIndex = mPatterns.get(tripHash);
        
        int patternIndexToSave = patternIndex;
        
        if (existingPatternIndex == null) {
            mPatterns.put(tripHash, patternIndexToSave);
            // Create new pattern
            
            int numRows = rows.size();
            int idx = 0;
            
            mPatternInsertStmt.setInt(1, patternIndexToSave);
            
            for (PatternRow row : rows) {
                mPatternInsertStmt.setInt(2, row.stopIndex);
                
                if (row.arrivalOffset >= 0) {
                    mPatternInsertStmt.setInt(3, row.arrivalOffset);
                }
                else {
                    mPatternInsertStmt.setNull(3, java.sql.Types.INTEGER);
                }
                
                if (row.departureOffset >= 0) {
                    mPatternInsertStmt.setInt(4, row.departureOffset);
                }
                else {
                    mPatternInsertStmt.setNull(4, java.sql.Types.INTEGER);
                }
                
                mPatternInsertStmt.setInt(5, idx + 1);
                mPatternInsertStmt.setInt(6, idx == numRows - 1 ? 1 : 0);
                
                if (row.headsign == null) {
                    mPatternInsertStmt.setNull(7, java.sql.Types.VARCHAR);
                }
                else {
                    mPatternInsertStmt.setString(7, row.headsign);
                }
                
                mPatternInsertStmt.setInt(8, row.pickup);
                mPatternInsertStmt.setInt(9, row.dropOff);
                
                mPatternInsertStmt.addBatch();
                
                idx++;
            }
            
            mPatternInsertStmt.executeBatch();
        }
        else {
            patternIndexToSave = existingPatternIndex;
        }
        
        result.close();
        
        mTripUpdateStmt.setInt(1, patternIndexToSave);
        mTripUpdateStmt.setInt(2, tripIndex);
        mTripUpdateStmt.execute();
        
        return patternIndex;
    }
    
    private PreparedStatement mShapeInsertStmt;

    private void optimizeShapes() throws SQLException {
        System.err.println("Optimizing shapes");

        runQuery("DROP TABLE IF EXISTS shapes_encoded");
        runQuery("CREATE TABLE shapes_encoded (shape_index INTEGER, shape_id TEXT, encoded_shape TEXT)");
        
        mShapeInsertStmt = mConnection.prepareStatement("INSERT INTO shapes_encoded (shape_index, shape_id, encoded_shape) VALUES (?, ?, ?)");
        
        
        PreparedStatement stmt = mConnection.prepareStatement("SELECT DISTINCT shape_id, shape_index FROM shapes");
        
        ResultSet result = stmt.executeQuery();
        
        int i = 1;
        
        while (result.next()) {
            String shapeId = result.getString(1);
            int shapeIndex = result.getInt(2);
        
            if (i % 100 == 0) {
                System.err.println(" " + i);
            }
            else {
                System.err.print(".");
            }

            optimizeShape(shapeId, shapeIndex);
            i++;
        }
        
        System.err.println();
        
        result.close();
        stmt.close();
        
        mShapeInsertStmt.close();
        
        runQuery("CREATE INDEX shapes_encoded_shape_index ON shapes_encoded (shape_index)");
        runQuery("CREATE INDEX shapes_encoded_shape_id ON shapes_encoded (shape_id)");
        runQuery("DROP TABLE shapes");
    }
    
    private void optimizeShape(String shapeId, int shapeIndex) throws SQLException {
        PreparedStatement stmt = mConnection.prepareStatement("SELECT shape_pt_lat, shape_pt_lon FROM shapes WHERE shape_index = ? ORDER BY shape_pt_sequence");
        stmt.setInt(1, shapeIndex);
        
        List<LatLng> path = new ArrayList<LatLng>();
        
        ResultSet result = stmt.executeQuery();
        
        while (result.next()) {
            LatLng point = new LatLng(result.getDouble(1), result.getDouble(2));
            path.add(point);
        }
        
        result.close();
        stmt.close();
        
        List<LatLng> reducedPath = reducePath(path);
        
        String encoded = Polyline.encode(reducedPath);
        
        mShapeInsertStmt.setInt(1, shapeIndex);
        mShapeInsertStmt.setString(2, shapeId);
        mShapeInsertStmt.setString(3, encoded);
        mShapeInsertStmt.executeUpdate();
    }
    
    private List<LatLng> reducePath(List<LatLng> path) {
        return DouglasPeuckerReducer.reduceWithTolerance(path, SHAPE_REDUCE_TOLERANCE);
    }
}
