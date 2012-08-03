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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static com.amazonaws.services.ec2.model.InstanceStateName.*;

/**
 * The {@link EC2ImageLaunchWrapper} is a true {@link ComputerLauncher} which is what
 * we are used to seeing when we configure a {@link DumbSlave}, however this class
 * is a wrapper and is not meant to be configured by a user.
 * <p/>
 * The role of {@link EC2ImageLaunchWrapper} is to manage the EC2 instance that is
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

    private String ami;

    private String instanceType;

    private String securityGroup;

    private String availabilityZone;

    private String keypairName;

    private int retryIntervalSeconds = 10;

    private int maxRetries = 60;

    private transient ComputerConnector computerConnector; /* factory for creating launcher based on hostname */

    private transient ComputerLauncher computerLauncher; /* the thing that we are wrapping. actually connects to the node as a hudson slave */

    private transient AmazonEC2Client ec2;

    private transient String curInstanceId;

    private transient boolean testMode = false;

    private transient boolean preLaunchOk = false;

    public EC2ImageLaunchWrapper(ComputerConnector computerConnector, String secretKey, String accessKey, String ami,
                                 String instanceType, String keypairName, String securityGroup, String availabilityZone) {
        this.ami = ami;
        this.computerConnector = computerConnector;
        // TODO: make a combobox for instance type in the slave config
        this.instanceType = instanceType;
        this.keypairName = keypairName;
        this.securityGroup = securityGroup;
        this.availabilityZone = availabilityZone;

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ec2 = new AmazonEC2Client(credentials);
        ec2.setEndpoint("us-west-1.ec2.amazonaws.com");

    }

    ////
    // EC2 Util methods
    //
    protected String launchInstanceFromImage() {
        RunInstancesRequest req = new RunInstancesRequest().withImageId(ami).withInstanceType(instanceType)
                .withKeyName(keypairName).withMinCount(1).withMaxCount(1);

        if (!StringUtils.isEmpty(securityGroup)) {
            req.withSecurityGroups(securityGroup);
        } else {
            req.withSecurityGroups("default");
        }

        if (!StringUtils.isEmpty(availabilityZone)) {
            req.setPlacement(new Placement(availabilityZone));
        }

        // Defaults to just stopping, when we're done with our slaves, we're done
        //req.setInstanceInitiatedShutdownBehavior("terminate");

        RunInstancesResult res = ec2.runInstances(req);
        Reservation rvn = res.getReservation();

        Instance instance = rvn.getInstances().get(0);
        return instance.getInstanceId();
    }

    protected InstanceStateName getInstanceState(String instanceId) {
        DescribeInstancesRequest descReq = new DescribeInstancesRequest().withInstanceIds(instanceId);
        Instance instance = ec2.describeInstances(descReq).getReservations().get(0).getInstances().get(0);
        return InstanceStateName.fromValue(instance.getState().getName());
    }

    protected String getInstancePublicHostName() {
        DescribeInstancesRequest descReq = new DescribeInstancesRequest().withInstanceIds(curInstanceId);
        Instance instance = ec2.describeInstances(descReq).getReservations().get(0).getInstances().get(0);
        return instance.getPublicDnsName();
    }
    
    public void stopInstance(PrintStream logger) throws InterruptedException {
        logger.println("EC2InstanceComputerLauncher: Stopping EC2 instance [" + curInstanceId + "] ...");
        if (testMode)
            return;

        StopInstancesResult stopResult = ec2.stopInstances(new StopInstancesRequest().withInstanceIds(curInstanceId));
        
        DescribeInstancesRequest request = new DescribeInstancesRequest().withInstanceIds(curInstanceId);
        Instance instance = ec2.describeInstances(request).getReservations().get(0).getInstances().get(0);
        
        InstanceStateName currentState = InstanceStateName.fromValue(instance.getState().getName());
        while ( currentState != Stopping) {
            currentState = InstanceStateName.fromValue(instance.getState().getName());
            logger.println(MessageFormat.format(
                    "instance [{0}] is " + currentState + ", waiting for it to stop.", curInstanceId));
            Thread.sleep(retryIntervalSeconds * 5000);
        }

    }

    public List<String> getAvailabilityZones() {
        DescribeAvailabilityZonesResult res = ec2.describeAvailabilityZones();
        ArrayList<String> ret = new ArrayList<String>();
        for (AvailabilityZone z : res.getAvailabilityZones()) {
            ret.add(z.getZoneName());
        }
        return ret;
    }

    public List<String> getSecurityGroups() {
        DescribeSecurityGroupsResult res = ec2.describeSecurityGroups();
        ArrayList<String> ret = new ArrayList<String>();
        for (SecurityGroup s : res.getSecurityGroups()) {
            ret.add(s.getGroupName());
        }
        return ret;
    }

    //
    ////

    @Override
    public boolean isLaunchSupported() {
        if (preLaunchOk) {
            // if the EC2 instance is up, launch supported should be determined
            // by the underlying launcher
            // this prevents Demand RetentionStrategy from attempting to spin up
            // endless EC2 instances
            boolean launchSupported = computerLauncher.isLaunchSupported();
            LOGGER.fine("EC2 instance is running, underlying launcher will now answer this question with " + launchSupported);
            return launchSupported;
        }
        // if the EC2 instance has not been started yet, return true here so we
        // seem like a legit auto-launchable launcher
        return true;
    }

    public boolean instanceIsRunning() {
        return (curInstanceId != null && getInstanceState(curInstanceId) == Running);
    }
    private void launchInstance(PrintStream logger) {
        logger.println("Starting instance: " + curInstanceId);

        ec2.startInstances( new StartInstancesRequest().withInstanceIds(curInstanceId));
        //String currentState = startResult.getStartingInstances().get(0).getCurrentState().getName();
    }

    @Override
    public void launch(SlaveComputer computer, TaskListener listener) throws IOException, InterruptedException {
        try {
            if (curInstanceId == null) {

                preLaunch(listener.getLogger());
                preLaunchOk = true;

            } else {

                final InstanceStateName currentInstanceState = getInstanceState(curInstanceId);

                if (currentInstanceState == Pending) {
                    //TODO:mpatel A better approach will be to shutdown and start the machine again associated with a timeout.
                    throw new IllegalStateException("EC2 Instance " + curInstanceId
                            + " is in Pending state. Not sure what to do here, try again?");
                } else if (currentInstanceState == Terminated) {
                    curInstanceId = null;
                    preLaunch(listener.getLogger());
                    preLaunchOk = true;
                } else if (currentInstanceState == Stopping || currentInstanceState == Stopped) {
                    preLaunch(listener.getLogger());
                    preLaunchOk = true;
                } else {
                    LOGGER.info("Skipping EC2 part of launch, since the instance is already running");
                }

            }
        } catch (IllegalStateException ise) {
            listener.error(ise.getMessage());
            return;
        } catch (AmazonServiceException ase) {
            listener.error(ase.getMessage());
            return;
        } catch (AmazonClientException ace) {
            listener.error(ace.getMessage());
            return;
        }

        LOGGER.info("EC2 instance " + curInstanceId
                + " has been created to serve as a Jenkins slave.  Passing control to computer launcher.");
        computerLauncher = computerConnector.launch(getInstancePublicHostName(), listener);
        computerLauncher.launch(computer, listener);
    }

    /**
     * Creates an EC2 instance given an AMI, readying it to serve as a Jenkins
     * slave once launch is called later
     *
     * @param logger where to write messages so the user will see them in the
     *               tailed log in Jenkins
     * @throws InterruptedException if the check status wait fails
     */
    public void preLaunch(PrintStream logger) throws InterruptedException {

        //logger.println("Creating new EC2 instance from AMI [" + ami + "]...");
        if (testMode)
            return;

        if (curInstanceId == null) {
            curInstanceId = launchInstanceFromImage();
        } else {
            launchInstance(logger);
        }

        int retries = 0;
        InstanceStateName state = null;

        while (++retries <= maxRetries) {

            logger.println(MessageFormat.format("checking state of instance [{0}]...", curInstanceId));

            state = getInstanceState(curInstanceId);

            logger.println(MessageFormat.format("state of instance [{0}] is [{1}]", curInstanceId, state.toString()));
            if (state == Running || state == Stopped) {
                logger.println(MessageFormat.format(
                        "instance [{0}] is " + state + ", proceeding to launching Jenkins on this instance", curInstanceId));
                Thread.sleep(retryIntervalSeconds * 6000);
                return;
            } else if (state == Pending) {
                logger.println(MessageFormat.format("instance [{0}] is pending, waiting for [{1}] seconds before retrying",
                        curInstanceId, retryIntervalSeconds));
                Thread.sleep(retryIntervalSeconds * 1000);
            } else if (state == Stopping) {
                logger.println(MessageFormat.format("instance [{0}] is Stopping. Waiting for [{1}] seconds before retrying",
                        curInstanceId, retryIntervalSeconds));
            } else {
                String msg = MessageFormat.format("instance [{0}] encountered unexpected state [{1}]. Aborting launch",
                        curInstanceId, state.toString());
                logger.println(msg);
                throw new IllegalStateException(msg);
            }
        }
        throw new IllegalStateException("Maximum Number of retries " + maxRetries + " exceeded. Aborting launch");
    }

    @Override
    public void afterDisconnect(SlaveComputer computer, TaskListener listener) {
        computerLauncher.afterDisconnect(computer, listener);
        try {
            stopInstance(listener.getLogger());
        } catch (InterruptedException ex) {
            listener.error(ex.getMessage());
        }
    }

    @Override
    public void beforeDisconnect(SlaveComputer computer, TaskListener listener) {

        super.beforeDisconnect(computer, listener);
    }

    @Override
    public Descriptor<ComputerLauncher> getDescriptor() {
        return computerLauncher.getDescriptor();
    }
}
