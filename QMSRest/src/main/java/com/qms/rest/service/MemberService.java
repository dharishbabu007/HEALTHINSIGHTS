package com.qms.rest.service;

import com.qms.rest.model.DimMember;
import com.qms.rest.repository.MemberRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MemberService {

    @Autowired
    private MemberRepository memberRepository;

    public DimMember getMember(String memberId)
    {
        System.out.println("Get Member for given member Id: "+memberId);
        return  memberRepository.findDimMemberByMemberId(memberId);
    }

}
