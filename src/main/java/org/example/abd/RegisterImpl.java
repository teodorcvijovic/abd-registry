package org.example.abd;

import org.example.abd.cmd.*;
import org.example.abd.quorum.Majority;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RegisterImpl<V> extends ReceiverAdapter implements Register<V>{

    private String name;

    private CommandFactory<V> factory;
    private JChannel channel;
    private boolean isWritable;

    private V value;
    private int label;
    private int max;

    private Majority quorumSystem;
    private List<CompletableFuture> replies;

    public RegisterImpl(String name) {
        this.name = name;
        this.factory = new CommandFactory<>();
    }

    public void open(boolean isWritable) throws Exception {
        this.isWritable = isWritable;

        this.open();
    }

    @Override
    public void viewAccepted(View view) {
        quorumSystem = new Majority(view);
    }

    // Client part

    @Override
    public void open() {
        label = -1;
        max = -1;

        // connect to the JChannel and register the RegisterImpl instance as a listener of the channel
        try {
            this.channel = new JChannel();
            channel.setReceiver(this);
            this.channel.connect(this.name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public V read() {
        ReadRequest<V> cmd = new ReadRequest<>();
        V v = execute(cmd);
        return v;
    }

    @Override
    public void write(V v) throws IllegalStateException {
        if (!isWritable) {
            // if the client executes write, but the register is not writable, the method throws a new IllegalStateException
            throw new IllegalStateException();
        }

        label = ++max;
        WriteRequest<V> cmd = new WriteRequest<>(v, label);
        execute(cmd);
    }

    @Override
    public void close() {
        this.channel.close();
    }

    private synchronized V execute(Command cmd){
        // we simply send the command to a quorum of replicas and not to all (as in the course)

        List<Address> replicas = quorumSystem.pickQuorum();
        replies = new ArrayList<>(); // update in receive

        for (Address replicaAddr : replicas) {
            try {
                CompletableFuture fut = new CompletableFuture();
                replies.add(fut);
                send(replicaAddr, cmd);
                fut.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (cmd.getClass().equals(ReadRequest.class)) {
            readRepair();
        }

        return value;
    }

    // Message handlers

    private void readRepair() {
        // broadcast write<value, label>
        WriteRequest<V> cmd = new WriteRequest<>(value, label);
        execute(cmd);
    }

    @Override
    public void receive(Message msg) {
        // replicas should now answer to requests by applying the core of the ABD algorithm

        if (msg.getObject().getClass().equals(ReadRequest.class)) {
            ReadRequest<V> req = (ReadRequest<V>) msg.getObject();
            ReadReply<V> reply = new ReadReply<>(value, label);
            send(msg.getSrc(), reply);
        }
        else if (msg.getObject().getClass().equals(WriteRequest.class)) {
            WriteRequest<V> req = (WriteRequest<V>) msg.getObject();
            if (req.getTag() > this.label) {
                this.label = req.getTag();
                this.value = req.getValue();
            }

            WriteReply reply = new WriteReply();
            send(msg.getSrc(), reply);
        }
        else if (msg.getObject().getClass().equals(ReadReply.class)) {
            ReadReply<V> reply = (ReadReply<V>) msg.getObject();
            V receivedValue = reply.getValue();
            int receivedLabel = reply.getTag();

            // return the receivedValue with max(receivedLabel)
            if (receivedLabel > this.label) {
                this.label = receivedLabel;
                this.value = receivedValue;
            }

            CompletableFuture<V> fut = replies.remove(0);
            fut.complete(null);
        }
        else if (msg.getObject().getClass().equals(WriteReply.class)) {
            // nothing to do, just notify the Future
            CompletableFuture<V> fut = replies.remove(0);
            fut.complete(null);
        }
    }

    private void send(Address dst, Command command) {
        try {
            Message message = new Message(dst,channel.getAddress(), command);
            channel.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
