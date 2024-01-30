package org.example.abd.quorum;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.*;

public class Majority {

    private Set<Address> allMembers = new HashSet<>();
    private int quorumSize;

    public Majority(View view){
        for(Address member : view.getMembers())
            allMembers.add(member);

        int n = allMembers.size();
        quorumSize = n / 2 + 1;
    }

    public int quorumSize(){
        return quorumSize;
    }

    public List<Address> pickQuorum() {
        List<Address> quorum = new ArrayList<>();
        Iterator<Address> iterator = allMembers.iterator();

        for (int i = 0; i < quorumSize; i++) {
            Address randomMemberAddr = iterator.next();
            quorum.add(randomMemberAddr);
        }

        return quorum;
    }

}
