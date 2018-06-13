/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.StringUtils;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.log.StartupLoggingConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
@Path("/status")
public class StatusResource
{

  @Inject
  private Injector injector;

  @GET
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Status doGet()
  {
    return new Status(
        Initialization.getLoadedImplementations(DruidModule.class),
        injector
    );
  }

  /**
   * This is an unsecured endpoint, defined as such in UNSECURED_PATHS in the service initiailization files
   * (e.g. CliOverlord, CoordinatorJettyServerInitializer)
   */
  @GET
  @Path("/health")
  @Produces(MediaType.APPLICATION_JSON)
  public boolean getHealth()
  {
    return true;
  }

  public static class Status
  {
    final String version;
    final List<ModuleVersion> modules;
    final Memory memory;
    final Injector injector;

    public Status(Collection<DruidModule> modules, Injector injector)
    {
      this.version = getDruidVersion();
      this.modules = getExtensionVersions(modules);
      this.memory = new Memory(Runtime.getRuntime());
      this.injector = injector;
    }

    private String getDruidVersion()
    {
      return Status.class.getPackage().getImplementationVersion();
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @JsonProperty
    public List<ModuleVersion> getModules()
    {
      return modules;
    }

    @JsonProperty
    public Memory getMemory()
    {
      return memory;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
      if (injector == null) {
        return ImmutableMap.of();
      }

      final StartupLoggingConfig startupLoggingConfig = injector.getInstance(StartupLoggingConfig.class);
      final Properties props = injector.getInstance(Properties.class);
      final Set<String> maskProperties = Sets.newHashSet(startupLoggingConfig.getMaskProperties());
      ImmutableMap.Builder builder = ImmutableMap.builder();

      for (String propertyName : Ordering.natural().sortedCopy(props.stringPropertyNames())) {
        String property = props.getProperty(propertyName);
        for (String masked : maskProperties) {
          if (propertyName.contains(masked)) {
            property = "<masked>";
            break;
          }
        }
        builder.put(propertyName, property);
      }
      return builder.build();
    }

    @JsonProperty
    public Map<String, Number> getDirectMemoryMetrics()
    {
      if (injector == null) {
        return ImmutableMap.of();
      }

      ImmutableMap.Builder builder = ImmutableMap.builder();
      for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
        builder.put("jvm/bufferpool/capacity", pool.getTotalCapacity());
        builder.put("jvm/bufferpool/used", pool.getMemoryUsed());
        builder.put("jvm/bufferpool/count", pool.getCount());
      }
      return builder.build();
    }

    @JsonProperty
    public Map<String, Number> getJvmMemMetrics()
    {
      if (injector == null) {
        return ImmutableMap.of();
      }

      ImmutableMap.Builder builder = ImmutableMap.builder();
      final Map<String, MemoryUsage> usages = ImmutableMap.of(
          "heap", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage(),
          "nonheap", ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage()
      );
      for (Map.Entry<String, MemoryUsage> entry : usages.entrySet()) {
        final String kind = entry.getKey();
        final MemoryUsage usage = entry.getValue();
        builder.put("jvm/mem/max", usage.getMax());
        builder.put("jvm/mem/committed", usage.getCommitted());
        builder.put("jvm/mem/used", usage.getUsed());
        builder.put("jvm/mem/init", usage.getInit());
      }

      // jvm/pool
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        final String kind = pool.getType() == MemoryType.HEAP ? "heap" : "nonheap";
        final MemoryUsage usage = pool.getUsage();
        builder.put("jvm/pool/max", usage.getMax());
        builder.put("jvm/pool/committed", usage.getCommitted());
        builder.put("jvm/pool/used", usage.getUsed());
        builder.put("jvm/pool/init", usage.getInit());
      }
      return builder.build();
    }

    @Override
    public String toString()
    {
      final String NL = System.getProperty("line.separator");
      StringBuilder output = new StringBuilder();
      output.append(StringUtils.format("Druid version - %s", version)).append(NL).append(NL);

      if (modules.size() > 0) {
        output.append("Registered Druid Modules").append(NL);
      } else {
        output.append("No Druid Modules loaded !");
      }

      for (ModuleVersion moduleVersion : modules) {
        output.append(moduleVersion).append(NL);
      }
      return output.toString();
    }

    /**
     * Load the unique extensions and return their implementation-versions
     *
     * @return map of extensions loaded with their respective implementation versions.
     */
    private List<ModuleVersion> getExtensionVersions(Collection<DruidModule> druidModules)
    {
      List<ModuleVersion> moduleVersions = new ArrayList<>();
      for (DruidModule module : druidModules) {
        String artifact = module.getClass().getPackage().getImplementationTitle();
        String version = module.getClass().getPackage().getImplementationVersion();

        moduleVersions.add(new ModuleVersion(module.getClass().getCanonicalName(), artifact, version));
      }
      return moduleVersions;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class ModuleVersion
  {
    final String name;
    final String artifact;
    final String version;

    public ModuleVersion(String name, String artifact, String version)
    {
      this.name = name;
      this.artifact = artifact;
      this.version = version;
    }

    @JsonProperty
    public String getName()
    {
      return name;
    }

    @JsonProperty
    public String getArtifact()
    {
      return artifact;
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @Override
    public String toString()
    {
      if (artifact == null || artifact.isEmpty()) {
        return StringUtils.format("  - %s ", name);
      } else {
        return StringUtils.format("  - %s (%s-%s)", name, artifact, version);
      }
    }
  }

  public static class Memory
  {
    final long maxMemory;
    final long totalMemory;
    final long freeMemory;
    final long usedMemory;

    public Memory(Runtime runtime)
    {
      maxMemory = runtime.maxMemory();
      totalMemory = runtime.totalMemory();
      freeMemory = runtime.freeMemory();
      usedMemory = totalMemory - freeMemory;
    }

    @JsonProperty
    public long getMaxMemory()
    {
      return maxMemory;
    }

    @JsonProperty
    public long getTotalMemory()
    {
      return totalMemory;
    }

    @JsonProperty
    public long getFreeMemory()
    {
      return freeMemory;
    }

    @JsonProperty
    public long getUsedMemory()
    {
      return usedMemory;
    }
  }
}
