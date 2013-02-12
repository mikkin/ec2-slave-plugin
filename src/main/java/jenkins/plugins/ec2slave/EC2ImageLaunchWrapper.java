/*
 * The MIT License
 * 
 * Copyright (c) 2011, Aaron Phillips
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package jenkins.plugins.ec2slave;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import hudson.model.Descriptor;
import hudson.model.TaskListener;
import hudson.slaves.ComputerConnector;
import hudson.slaves.ComputerLauncher;
import hudson.slaves.DumbSlave;
import hudson.slaves.SlaveComputer;

import java.io.IOException;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.logging.Logger;

import static com.amazonaws.services.ec2.model.InstanceStateName.*;

/**
 * The {@link EC2ImageLaunchWrapper} is a true {@link ComputerLauncher} which is what
 * we are used to seeing when we configure a {@link DumbSlave}, however this class
 * is a wrapper and is not meant to be configured by a user.
 * <p/>
 * The role of {@link EC2ImageLaunchWrapper} is to manage the EC2 instanceId that is
 * bound to the Jenkins slave.  This class takes care of startup and shutdown hooks,
 * communicating with Amazon via the Java AWS client.
 * <p/>
 * The other part of it's role is to expose the actual {@link ComputerLauncher}
 * which is derived from the user configured {@link ComputerConnector}
 *
 * @author Aaron Phillips
 */
public class EC2ImageLaunchWrapper extends ComputerLauncher {

    private static final Logger LOGGER = Logger.getLogger(EC2ImageLaunchWrapper.class.getName());
    private static final int RETRY_INTERVAL_SECONDS = 10;
    private static final int MAX_RETRIES = 60;

    private final String instanceId;

    private transient ComputerConnector computerConnector; /* factory for creating launcher based on hostname */
    private transient ComputerLauncher computerLauncher; /* the thing that we are wrapping. actually connects
                                                            to the node as a hudson slave */
    private transient AmazonEC2Client ec2;
    private transient boolean testMode = false;

    /**
     *
     * @param computerConnector
     * @param accessKey
     * @param secretKey
     * @param instanceId
     */
    public EC2ImageLaunchWrapper(ComputerConnector computerConnector, String accessKey, String secretKey,
                                    String instanceId) {

        this.instanceId = instanceId;
        this.computerConnector = computerConnector;

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ec2 = new AmazonEC2Client(credentials);
        ec2.setEndpoint("ec2.us-west-1.amazonaws.com");
    }

    /**
     *
     * @param computer
     * @param listener
     * @throws InterruptedException
     * @throws IOException
     */
    @Override
    public void launch(SlaveComputer computer, TaskListener listener) throws InterruptedException, IOException {
        
        final InstanceStateName currentInstanceState = getInstanceState(instanceId);

        if (currentInstanceState == Pending || currentInstanceState == Terminated) {
            //TODO:mpatel A better approach will be to shutdown and start the machine again associated with a timeout.
            throw new IllegalStateException("EC2 Instance " + instanceId
                    + " is in " + currentInstanceState + " state. Not sure what to do here, try again?");
        } else if (currentInstanceState == Stopping || currentInstanceState == Stopped) {
            waitForState(Stopped, listener.getLogger());
            launchInstance(listener.getLogger());
            waitForState(Running, listener.getLogger());
        } else if (currentInstanceState == Running) {
            Thread.sleep(RETRY_INTERVAL_SECONDS * 6000);
        } else {
            LOGGER.info("Skipping EC2 part of launch, since the instance is already running");
        }

        listener.getLogger().println("EC2 instance " + instanceId
                + " has been created to serve as a Jenkins slave.  Passing control to computer launcher.");
        computerLauncher = computerConnector.launch(getInstancePublicHostName(), listener);
        computerLauncher.launch(computer, listener);
    }

    @Override
    public void afterDisconnect(SlaveComputer computer, TaskListener listener) {

        computerLauncher.afterDisconnect(computer, listener);
        stopInstance(listener.getLogger());
    }

    @Override
    public void beforeDisconnect(SlaveComputer computer, TaskListener listener) {
        super.beforeDisconnect(computer, listener);
        stopInstance(listener.getLogger());
    }

    @Override
    public Descriptor<ComputerLauncher> getDescriptor() {
        return computerLauncher.getDescriptor();
    }

    @Override
    public boolean isLaunchSupported() {

        return true;
    }

    private void waitForState(InstanceStateName expectedState, PrintStream logger) throws InterruptedException {

        int retries = 0;
        InstanceStateName state = null;

        while (++retries <= MAX_RETRIES) {

            logger.println(MessageFormat.format("checking state of instance [{0}]...", instanceId));

            state = getInstanceState(instanceId);
            logger.println(MessageFormat.format("state of instance [{0}] is [{1}]", instanceId, state.toString()));

            if (state == expectedState) {
                logger.println(MessageFormat.format("instance [{0}] is " + state + ". Proceeding...", instanceId));
                if (state == Running)
                    Thread.sleep(RETRY_INTERVAL_SECONDS * 6000);
                return;
            } else if (state == Pending) {
                logger.println(MessageFormat.format("instance [{0}] is pending, waiting for [{1}] seconds before"
                        + " retrying", instanceId, RETRY_INTERVAL_SECONDS));
                Thread.sleep(RETRY_INTERVAL_SECONDS * 1000);
            } else if (state == Stopping) {
                logger.println(MessageFormat.format("instance [{0}] is Stopping. Waiting for [{1}] seconds before" +
                        " retrying", instanceId, RETRY_INTERVAL_SECONDS));
            } else {
                String msg = MessageFormat.format("instance [{0}] encountered unexpected state [{1}]. Aborting launch",
                        instanceId, state.toString());
                logger.println(msg);
                throw new IllegalStateException(msg);
            }
        }
        throw new IllegalStateException("Maximum Number of retries " + MAX_RETRIES + " exceeded. Aborting launch");
    }

    protected InstanceStateName getInstanceState(String instanceId) {
        DescribeInstancesRequest descReq = new DescribeInstancesRequest().withInstanceIds(instanceId);
        Instance instance = ec2.describeInstances(descReq).getReservations().get(0).getInstances().get(0);
        return InstanceStateName.fromValue(instance.getState().getName());
    }

    protected String getInstancePublicHostName() {
        DescribeInstancesRequest descReq = new DescribeInstancesRequest().withInstanceIds(instanceId);
        Instance instance = ec2.describeInstances(descReq).getReservations().get(0).getInstances().get(0);
        return instance.getPublicDnsName();
    }
    
    private void stopInstance(PrintStream logger) {
        logger.println("EC2InstanceComputerLauncher: Stopping EC2 instance [" + instanceId + "] ...");
        if (testMode)
            return;

        InstanceStateChange changedState = new InstanceStateChange().withInstanceId(instanceId);

        final StopInstancesResult stopResult =
                ec2.stopInstances(new StopInstancesRequest().withInstanceIds(instanceId));

        stopResult.withStoppingInstances(changedState);
    }

    public boolean instanceIsRunning() {
        return (instanceId != null && getInstanceState(instanceId) == Running);
    }

    private void launchInstance(PrintStream logger) {
        logger.println("Starting instance: " + instanceId);
        ec2.startInstances(new StartInstancesRequest().withInstanceIds(instanceId));
    }


}
