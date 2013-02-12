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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import hudson.Extension;
import hudson.model.Computer;
import hudson.model.Descriptor;
import hudson.model.Descriptor.FormException;
import hudson.model.Hudson;
import hudson.model.Slave;
import hudson.slaves.*;
import hudson.util.FormValidation;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * The {@link EC2ImageSlave} is a slave in the same way {@link DumbSlave} is, i.e.
 * you can create one of these through the normal node creation process.  What makes
 * this class different from DumbSlave is that it binds a Jenkins {@link Computer}
 * not to a running machine, but to an Amazon EC2 instance which is launched from
 * a specified Amazon Machine Image (AMI).  Configuring the {@link EC2ImageSlave}
 * involves specifying all information needed to:
 * <ol><li>launch the AMI</li>
 * <li>connect to the resulting image using the user selected {@link ComputerConnector}</li>
 * <li>launch Jenkins as a slave on the EC2 instance
 *
 * @author Aaron Phillips
 */
public final class EC2ImageSlave extends Slave {
    private static final long serialVersionUID = -3392496004371742586L;

    private static final Logger LOGGER = Logger.getLogger(EC2ImageSlave.class.getName());

    private String accessKey, secretKey, instanceId;

    private transient EC2ImageLaunchWrapper ec2ImageLaunchWrapper;

    /**
     * The connector that will factory the {@link ComputerLauncher} after providing
     * it the public DNS hostname
     */
    private ComputerConnector computerConnector;

    @DataBoundConstructor
    public EC2ImageSlave(String secretKey, String accessKey, String instanceId, String name, String nodeDescription,
                         String remoteFS, String numExecutors, Mode mode, String labelString,
                         ComputerConnector computerConnector, RetentionStrategy retentionStrategy,
                         List<? extends NodeProperty<?>> nodeProperties) throws FormException, IOException {

        super(name, nodeDescription, remoteFS, numExecutors, mode, labelString,
                null /* null launcher because we create it dynamically in getLauncher */, retentionStrategy, nodeProperties);

        this.computerConnector = computerConnector;

        if (ec2ImageLaunchWrapper != null && ec2ImageLaunchWrapper.instanceIsRunning()) {
            final String msg = "You cannot change EC2 configuration while the instance is running.";

            if (!secretKey.equals(this.secretKey))
                throw new FormException(msg, "secretKey");
            if (!accessKey.equals(this.accessKey))
                throw new FormException(msg, "accessKey");
            if (!instanceId.equals(this.instanceId))
                throw new FormException(msg, "imageId");
        }
        this.secretKey = secretKey.trim();
        this.accessKey = accessKey.trim();
        this.instanceId = instanceId.trim();
    }

    @Override
    public ComputerLauncher getLauncher() {
        //Letting the user choose ComputerConnector and returning
        //computerConnector.launch(..) here which will return a ComputerLauncher with the
        //hostname already set.  This implies that the EC2ImageSlave config will be displaying
        //Computer *Connector* descriptor stuff rather than *Launcher*

        ec2ImageLaunchWrapper = new EC2ImageLaunchWrapper(computerConnector, accessKey, secretKey, instanceId);
        setLauncher(ec2ImageLaunchWrapper);

        return ec2ImageLaunchWrapper;
    }

    @Extension
    public static final class DescriptorImpl extends SlaveDescriptor {
        public String getDisplayName() {
            return "EC2 Image Slave";
        }

        public FormValidation doTestConnection(@QueryParameter String accessKey, @QueryParameter String secretKey) {
            try {
                AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
                AmazonEC2Client ec2 = new AmazonEC2Client(credentials);
                ec2.setEndpoint("ec2.us-west-1.amazonaws.com");
                final DescribeAvailabilityZonesResult describeAvailabilityZonesResult = ec2.describeAvailabilityZones();
                LOGGER.info(describeAvailabilityZonesResult.toString());
                return FormValidation.ok("Success");
            } catch (AmazonServiceException e) {
                LOGGER.warning("Failed to check EC2 credential: " + e.getMessage());
                return FormValidation.error(e.getMessage());
            }
        }

        /**
         *
         * @param accessKey - AWS access key.
         * @param secretKey - AWS secret key.
         * @param instanceId
         * @return
         */
        public FormValidation doValidateInstance(@QueryParameter String accessKey, @QueryParameter String secretKey,
                                                 @QueryParameter String instanceId) {
            
            FormValidation val = doTestConnection(accessKey, secretKey);
            if (val.kind == FormValidation.Kind.ERROR) {
                return val;
            }
            LOGGER.info("Validating instance");
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonEC2Client ec2 = new AmazonEC2Client(credentials);
            ec2.setEndpoint("us-west-1.ec2.amazonaws.com");

            DescribeInstancesResult instanceResult = ec2.describeInstances(
                    new DescribeInstancesRequest().withInstanceIds(instanceId));
            LOGGER.info("Instance result: " + instanceResult.toString());
            if (instanceResult.getReservations().size() > 0 &&
                    instanceResult.getReservations().get(0).getInstances().size() > 0 ) {
                
                final Instance instance = instanceResult.getReservations().get(0).getInstances().get(0);

                LOGGER.info("Instance " + instanceId + " is in " + instance.getState().getName()
                        + " state");
                return FormValidation.ok("Instance " + instanceId + " is in " + instance.getState().getName()
                                            + " state");

            }
            return FormValidation.error("No such instance: " + instanceId);
        }

        public static List<Descriptor<ComputerConnector>> getComputerConnectorDescriptors() {
            return Hudson.getInstance().<ComputerConnector, Descriptor<ComputerConnector>>getDescriptorList(
                    ComputerConnector.class);
        }
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public ComputerConnector getComputerConnector() {
        return computerConnector;
    }

}
