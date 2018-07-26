package com.qms.rest.service;

import com.qms.rest.exception.ProgramCreatorException;
import com.qms.rest.model.DimMember;
import com.qms.rest.util.QMSConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.qms.rest.query.QueryConstant.DIM_MEMBEER_QUERY_ID;
import static com.qms.rest.query.QueryConstant.DIM_MEMBEER_QUERY_NAME;

@Service
public class MemberService {

    @Autowired
    private QMSConnection qmsConnection;

    public List<DimMember> findDimMembers(String memberId) {
        List<DimMember> memberList = null;
        System.out.println("Get Member for given member Id: " + memberId);

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection connection = null;

        boolean isNumeric = memberId.chars().allMatch(Character::isDigit);

        String sqlQuery = isNumeric ? DIM_MEMBEER_QUERY_ID : DIM_MEMBEER_QUERY_NAME;

        try {
            connection = qmsConnection.getHiveConnection();
            System.out.println("Service after connection: " + connection);
            statement = connection.prepareStatement(sqlQuery);
            memberId = "%"+memberId.toLowerCase()+"%";
            if(isNumeric){
                statement.setString(1, memberId);
            }
            else{
                statement.setString(1, memberId);
                statement.setString(2, memberId);
                statement.setString(3, memberId);
            }
            resultSet = statement.executeQuery();
            memberList = convertToDimMember(resultSet);

        } catch (Exception e) {
            System.out.println("Error: Exception while fetching dimMember for id: " + memberId);
            e.printStackTrace();
        } finally {
            System.out.println("About to close connection in find dim members");
            qmsConnection.closeJDBCResources(resultSet, statement, connection);
        }

        if (memberList.size() == 0) {
            System.out.println("Warn: There is no entry found for given search key: " + memberId);
            throw new ProgramCreatorException("There is no entry found for given search key: " + memberId);
        }
        return memberList;
    }

    private List<DimMember> convertToDimMember(ResultSet resultSet) throws SQLException {
        List<DimMember> memberList = new ArrayList<>();

        while (resultSet.next()) {
            DimMember dimMember = DimMember.builder()
                    .memberId(resultSet.getString("MEMBER_ID"))
                    .firstName(resultSet.getString("FIRST_NAME"))
                    .middelName(resultSet.getString("MIDDLE_NAME"))
                    .lastName(resultSet.getString("LAST_NAME"))
                    .build();
            memberList.add(dimMember);
        }
        System.out.println("Total fetched record : " + memberList.size());
        return memberList;
    }

}
