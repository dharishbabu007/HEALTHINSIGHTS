package com.qms.rest.service;

import com.qms.rest.model.DimMember;
import com.qms.rest.repository.MemberRepository;
import com.qms.rest.util.QMSConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.qms.rest.query.QueryConstant.DIM_MEMBEER_QUERY;

@Service
public class MemberService {

    @Autowired
    private QMSConnection qmsConnection;

    public List<DimMember> findDimMembers(String memberId)
    {
        List<DimMember> memberList = null;
        System.out.println("Get Member for given member Id: "+memberId);

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Connection connection = null;

        try {
            connection = qmsConnection.getHiveConnection();
            System.out.println("Service after connection: " + connection);
            statement = connection.prepareStatement(DIM_MEMBEER_QUERY);
            statement.setString(1, memberId);
            statement.setString(2, memberId);
            resultSet = statement.executeQuery();
            memberList = convertToDimMember(resultSet);

        } catch (Exception e) {
            System.out.println("Error: Exception while fetching dimMember for id: "+memberId);
            e.printStackTrace();
        } finally {
            System.out.println("About to close connection in find dim members");
            qmsConnection.closeJDBCResources(resultSet, statement, connection);
        }
        return memberList;
    }

    private List<DimMember> convertToDimMember(ResultSet resultSet) throws SQLException {
        List<DimMember> memberList = new ArrayList<>();

        while (resultSet.next()){
            DimMember dimMember = DimMember.builder()
                    .firstName(resultSet.getString("FIRST_NAME"))
                    .lastName(resultSet.getString("LAST_NAME"))
                    .memberId(resultSet.getString("MEMBER_ID"))
                    .build();
            memberList.add(dimMember);
        }
        System.out.println("Total fetched record : "+memberList.size());
        return memberList;
    }

}
