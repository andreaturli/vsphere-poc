package vsphere.poc.network;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.rmi.RemoteException;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.compute.reference.ComputeServiceConstants;
import org.jclouds.logging.Logger;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.ssh.SshClient;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.jclouds.vsphere.BaseVSphereClientLiveTest;
import org.jclouds.vsphere.compute.VSphereComputeServiceAdapter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Module;
import com.vmware.vim25.HostIpConfig;
import com.vmware.vim25.HostNetworkPolicy;
import com.vmware.vim25.HostPortGroupSpec;
import com.vmware.vim25.HostVirtualNic;
import com.vmware.vim25.HostVirtualNicSpec;
import com.vmware.vim25.HostVirtualSwitch;
import com.vmware.vim25.HostVirtualSwitchBondBridge;
import com.vmware.vim25.HostVirtualSwitchSpec;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.LinkDiscoveryProtocolConfig;
import com.vmware.vim25.PhysicalNic;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.VirtualDevice;
import com.vmware.vim25.VirtualDeviceConfigSpec;
import com.vmware.vim25.VirtualDeviceConfigSpecOperation;
import com.vmware.vim25.VirtualEthernetCard;
import com.vmware.vim25.VirtualEthernetCardNetworkBackingInfo;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.VirtualMachineConfigSpec;
import com.vmware.vim25.VirtualPCNet32;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostNetworkSystem;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import com.vmware.vim25.mo.VirtualMachine;

/**
 * 
 * This test case will do the following:
 * - create a new vNetwork (with a new vSwitch and 2 port groups) ONLY if an unclaimed physical adapter is available 
 *   on the host
 * - deploy 1 virtual machine from a default template
 * - reconfigure the vm to use the new vNetwork
 * - destroy the vm
 * - remove the vSwitch 
 * 
 * 
 * @author Andrea Turli
 */
@Test(groups = "live", singleThreaded = true, testName = "XeroxPocTest")
public class VSpherePocTest extends BaseVSphereClientLiveTest {


   private static final String SUBNET_NETMASK = "255.255.252.0";

   @Resource
   @Named(ComputeServiceConstants.COMPUTE_LOGGER)
   protected Logger logger = Logger.NULL;

   ComputeServiceContext context;
   private Set<? extends NodeMetadata> nodes;

   private HostSystem host;
     
   private static final String CLUSTER_NAME = "test-launch-cluster";;
   private static final String VM_PORT_GROUP_NAME = "Test VirtualMachine PortGroup";
   private static final String MANAGEMENT_PORT_GROUP_NAME = "Test Management PortGroup";
   private static final String SWITCH_NAME = "Test Switch";
   private static final String SUBNET_IP_ADDRESS = "10.20.143.204";
   private static final String MAC_ADDRESS = "00:50:56:7d:5e:0b";

   @Inject
   protected VSphereComputeServiceAdapter adapter;

   @Parameters({"vsphere.identity", "vsphere.credential", "vsphere.endpoint"})
   @BeforeClass
   public void setUp(String identity, String credential, String endpoint) throws Exception {
      // jclouds initializations
      nodes = Sets.newLinkedHashSet();
      context = ContextBuilder.newBuilder("vsphere")
            .modules(ImmutableSet.<Module> of(new SLF4JLoggingModule(), new SshjSshClientModule()))
            .build(ComputeServiceContext.class);
      // vijava initializations
      ServiceInstance serviceInstance = new ServiceInstance(new URL(endpoint), identity, credential, true);
      // assuming we have only one host on our vsphere (one ESXi)
      Folder rootFolder = serviceInstance.getRootFolder();
      host = (HostSystem) new InventoryNavigator(rootFolder).searchManagedEntities("HostSystem")[0];
   }
   
   @AfterClass
   public void tearDown() throws Exception {
      Set<? extends NodeMetadata> nodesDestroyed = context.getComputeService().destroyNodesMatching(
            new Predicate<NodeMetadata>() {

               public boolean apply(NodeMetadata input) {
                  return input.getId().contains(CLUSTER_NAME);
               }

               @Override
               public String toString() {
                  return "ContainsClusterName()";
               }
            });
      assertEquals(nodesDestroyed.size(), nodes.size(), "wrong number of destroyed nodes");
      removeVirtualSwitch(host, SWITCH_NAME, MANAGEMENT_PORT_GROUP_NAME);
   }
   
   @Test 
   public void createVNetwork() throws Exception {
      String unclaimedPhysicalAdapter = null;
      Set<String> unclaimedPhysicalAdapters = listUnclaimedPhysicalAdapters(host);
      Optional<String> unclaimedPhysicalAdapterOptional = Iterables.tryFind(unclaimedPhysicalAdapters, Predicates.notNull());
      if(unclaimedPhysicalAdapterOptional.isPresent()) {
         unclaimedPhysicalAdapter = Iterables.getLast(Splitter.on("-").split(unclaimedPhysicalAdapterOptional.get()));
      } else {
         Assert.fail();
         return;
      }

      // add a virtual switch 
      HostVirtualSwitchSpec spec = new HostVirtualSwitchSpec();
      spec.setNumPorts(8);
      HostNetworkSystem hostNetworkSystem = host.getHostNetworkSystem();
      hostNetworkSystem.addVirtualSwitch(SWITCH_NAME, spec);

      // add a VirtualMachine Port Group 
      HostPortGroupSpec vmPortGroupSpec = createPortGroup(VM_PORT_GROUP_NAME, SWITCH_NAME);
      hostNetworkSystem.addPortGroup(vmPortGroupSpec);

      HostPortGroupSpec managementPortGroupSpec = createPortGroup(MANAGEMENT_PORT_GROUP_NAME, SWITCH_NAME);
      hostNetworkSystem.addPortGroup(managementPortGroupSpec);

      // add a virtual NIC to VMKernel 
      HostVirtualNicSpec hostVirtualNicSpec = new HostVirtualNicSpec();
      hostVirtualNicSpec.setMac(MAC_ADDRESS);
      HostIpConfig hic = new HostIpConfig();
      hic.setDhcp(false);
      hic.setIpAddress(SUBNET_IP_ADDRESS);
      hic.setSubnetMask(SUBNET_NETMASK);
      hostVirtualNicSpec.setIp(hic);
      hostNetworkSystem.addVirtualNic(MANAGEMENT_PORT_GROUP_NAME, hostVirtualNicSpec);

      // assign a physicalNIC to the new vSwitch
      assertTrue(addPhysicalNic(hostNetworkSystem, SWITCH_NAME, unclaimedPhysicalAdapter));
   }

   @Test(dependsOnMethods = { "createVNetwork" })
   public void testLaunchCluster() throws RunNodesException {
      int numNodes = 1;
      nodes = context.getComputeService().createNodesInGroup(CLUSTER_NAME, numNodes,
            TemplateOptions.Builder.overrideLoginUser("toor").overrideLoginPassword("password"));
      assertEquals(numNodes, nodes.size(), "wrong number of nodes");
      for (NodeMetadata node : nodes) {
         assertTrue(node.getGroup().equals(CLUSTER_NAME));
         logger.debug("Created Node: %s", node);
         SshClient client = context.utils().sshForNode().apply(node);
         try {
            client.connect();
            ExecResponse hello = client.exec("echo hello");
            assertEquals(hello.getOutput().trim(), "hello");
         } finally {
            if (client != null)
               client.disconnect();
         }
      }
   }

   @Test(dependsOnMethods = { "testLaunchCluster" })
   public void testReconfigureNIC() throws Exception {
      VirtualMachineConfigSpec vmConfigSpec = new VirtualMachineConfigSpec();
      for (NodeMetadata node : nodes) {
         String vmName = node.getName();
         VirtualMachine vm = adapter.getNode(vmName);
         VirtualDeviceConfigSpec nicSpec = getNICDeviceConfigSpec(vm, "Edit", "Network Adapter 1");
         if (nicSpec != null) {
            VirtualDeviceConfigSpec[] nicSpecArray = { nicSpec };
            vmConfigSpec.setDeviceChange(nicSpecArray);
            Task task = vm.reconfigVM_Task(vmConfigSpec);
            assertTrue(task.waitForTask().equals(Task.SUCCESS));
         }
      }
   }

   private static VirtualDeviceConfigSpec getNICDeviceConfigSpec(VirtualMachine vm, String ops, String value)
         throws Exception {
      VirtualDeviceConfigSpec nicSpec = new VirtualDeviceConfigSpec();
      VirtualMachineConfigInfo vmConfigInfo = vm.getConfig();

      if (ops.equalsIgnoreCase("Add")) {
         nicSpec.setOperation(VirtualDeviceConfigSpecOperation.add);
         VirtualEthernetCard nic = new VirtualPCNet32();
         VirtualEthernetCardNetworkBackingInfo nicBacking = new VirtualEthernetCardNetworkBackingInfo();
         nicBacking.setDeviceName(VM_PORT_GROUP_NAME);
         nic.setAddressType("generated");
         nic.setBacking(nicBacking);
         nic.setKey(4);
         nicSpec.setDevice(nic);
      } else if (ops.equalsIgnoreCase("Remove")) {
         VirtualEthernetCard nic = null;
         VirtualDevice[] test = vmConfigInfo.getHardware().getDevice();
         nicSpec.setOperation(VirtualDeviceConfigSpecOperation.remove);
         for (int k = 0; k < test.length; k++) {
            if (test[k].getDeviceInfo().getLabel().equalsIgnoreCase(value)) {
               nic = (VirtualEthernetCard) test[k];
            }
         }
         if (nic != null) {
            nicSpec.setDevice(nic);
         } else {
            return null;
         }
      } else if (ops.equalsIgnoreCase("Edit")) {
         VirtualEthernetCard nic = null;
         VirtualDevice[] test = vmConfigInfo.getHardware().getDevice();
         nicSpec.setOperation(VirtualDeviceConfigSpecOperation.edit);
         for (int k = 0; k < test.length; k++) {
            if (test[k].getDeviceInfo().getLabel().equalsIgnoreCase(value)) {
               nic = (VirtualEthernetCard) test[k];
               VirtualEthernetCardNetworkBackingInfo nicBacking = new VirtualEthernetCardNetworkBackingInfo();
               nicBacking.setDeviceName(VM_PORT_GROUP_NAME);
               nic.setAddressType("generated");
               nic.setBacking(nicBacking);
            }
         }
         if (nic == null) {
            throw new Exception("Cannot EDIT the NIC");
         }
         nicSpec.setDevice(nic);
      }
      return nicSpec;
   }

   private static HostPortGroupSpec createPortGroup(String vmPortGroupName, String switchName) {
      HostPortGroupSpec vmPortGroupSpec = new HostPortGroupSpec();
      vmPortGroupSpec.setName(vmPortGroupName);
      vmPortGroupSpec.setVlanId(0); // not associated with a VLAN
      vmPortGroupSpec.setVswitchName(switchName);
      vmPortGroupSpec.setPolicy(new HostNetworkPolicy());
      return vmPortGroupSpec;
   }

   /**
    * 
    * from http://vijava.svn.sourceforge.net/viewvc/vijava/tags/2.0u1/src/com/vmware/vim25/mo/samples/network/
    * 
    * @param hostNetworkSystem
    * @param vSwitchName
    * @param pNICs
    * @return
    */
   public static Boolean addPhysicalNic(HostNetworkSystem hostNetworkSystem, String vSwitchName, String... pNICs) {
      HostVirtualSwitch[] vSwitches = hostNetworkSystem.getNetworkInfo().getVswitch();
      HostVirtualSwitch vSwitch = null;

      for (HostVirtualSwitch tmpVSwitch : vSwitches) {
         if (tmpVSwitch.getName().equals(vSwitchName))
            vSwitch = tmpVSwitch;
         else
            continue;
         break;
      }
      
      if (vSwitch == null) 
         return false;
      
      HostVirtualSwitchSpec hostVirtualSwitchSpec = vSwitch.getSpec();
      HostVirtualSwitchBondBridge hvsbb = (HostVirtualSwitchBondBridge) hostVirtualSwitchSpec.getBridge();
      if (hvsbb == null) {
         hvsbb = new HostVirtualSwitchBondBridge();
         LinkDiscoveryProtocolConfig ldpc = new LinkDiscoveryProtocolConfig();
         ldpc.setOperation("listen");
         ldpc.setProtocol("cdp");
         hvsbb.setLinkDiscoveryProtocolConfig(ldpc);
      }
      
      int nicCount = (hvsbb.getNicDevice() == null) ? 0 : hvsbb.getNicDevice().length;
      String[] PhysicalNICs = new String[nicCount + pNICs.length];
      int pnIndex = 0;
      if (hvsbb.getNicDevice() != null)
         for (String pNIC : hvsbb.getNicDevice())
            PhysicalNICs[pnIndex++] = pNIC;
      for (String pNIC : pNICs)
         PhysicalNICs[pnIndex++] = pNIC;
      hvsbb.setNicDevice(PhysicalNICs);
      hostVirtualSwitchSpec.setBridge(hvsbb);

      hostVirtualSwitchSpec.getPolicy().getNicTeaming().getNicOrder().setActiveNic(PhysicalNICs);
      try {
         hostNetworkSystem.updateVirtualSwitch(vSwitchName, hostVirtualSwitchSpec);
         return true;
      } catch (Exception e) {
         throw Throwables.propagate(e);
      }
  }

   private static void removeVirtualSwitch(HostSystem host, String vSwitchName, String portGroupName) throws RemoteException {
      removeVirtualNic(host, portGroupName);
      try {
         HostNetworkSystem hostNetworkSystem = host.getHostNetworkSystem();
         HostVirtualSwitch[] vSwitches = hostNetworkSystem.getNetworkInfo().getVswitch();
         for (HostVirtualSwitch vSwitch : vSwitches) {
            if(vSwitch.getName().equals(vSwitchName)) {
               hostNetworkSystem.removeVirtualSwitch(vSwitchName);
            }
         }
      } catch (Exception e) {
         Assert.fail();
      }
   }
   
   private static void removeVirtualNic(HostSystem host, String portGroupName) {
      try {
         HostNetworkSystem hns = host.getHostNetworkSystem();
         HostVirtualNic[] hvns = hns.getNetworkInfo().getVnic();
         if (hvns == null) {
            return;
         }

         boolean found = false;
         for (int i = 0; i < hvns.length; i++) {
            HostVirtualNic nic = hvns[i];
            String portGroup = nic.getPortgroup();
            if (portGroup != null && portGroup.equals(portGroupName)) {
               found = true;
               hns.removeVirtualNic(nic.getDevice());
            }
         }
         if (!found) {
            Assert.fail();
         }
      } catch (Exception e) {
         Assert.fail();
      }
   }
   
   private static Set<String> listUnclaimedPhysicalAdapters(HostSystem host) throws InvalidProperty, RuntimeFault,
         RemoteException {

      HostNetworkSystem hostNetworkSystem = host.getHostNetworkSystem();
      
      Set<String> availablePhysicalNICs = Sets.newLinkedHashSet();
      for (PhysicalNic physicalNic : hostNetworkSystem.getNetworkInfo().getPnic()) {
         availablePhysicalNICs.add(physicalNic.getKey());
      }

      Set<String> claimedPhysicalNICs = Sets.newLinkedHashSet();
      HostVirtualSwitch[] vSwitches = hostNetworkSystem.getNetworkInfo().getVswitch();
      for (HostVirtualSwitch vSwitch : vSwitches) {
         if (vSwitch != null && vSwitch.getPnic() != null) {
            for (String pNic : vSwitch.getPnic()) {
               claimedPhysicalNICs.add(pNic);
            }
         }
      }
      return Sets.difference(availablePhysicalNICs, claimedPhysicalNICs);
   }
}
