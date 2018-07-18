package com.qms.rest.repository;


import com.qms.rest.model.DimMember;
import com.qms.rest.model.QualityProgram;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource
public interface MemberRepository extends CrudRepository<DimMember, Integer> {

    DimMember findDimMemberByMemberId(@Param("memberId") String memberId);
}
