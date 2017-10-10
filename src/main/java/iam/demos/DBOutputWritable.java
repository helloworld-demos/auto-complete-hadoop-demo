package iam.demos;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{

    private String startingPhrase;
    private String followingWord;
    private int count;

    public DBOutputWritable(String startingPhrase, String followingWord, int count) {
        this.startingPhrase = startingPhrase;
        this.followingWord = followingWord;
        this.count= count;
    }

    public void readFields(ResultSet rs) throws SQLException {
        this.startingPhrase = rs.getString(1);
        this.followingWord = rs.getString(2);
        this.count = rs.getInt(3);
    }

    public void write(PreparedStatement rs) throws SQLException {
        rs.setString(1, startingPhrase);
        rs.setString(2, followingWord);
        rs.setInt(3, count);
    }
}
