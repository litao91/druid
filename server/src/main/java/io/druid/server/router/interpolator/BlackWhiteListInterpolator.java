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
package io.druid.server.router.interpolator;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.Query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * It works like follows
 * If the whitelist is null, everything is in whitelist.
 * If the blacklist is null, assume that the blocklist is empty
 *
 * We check the rules in the following order
 * 1. If it's in blacklist, don't apply the interpolator
 * 2. If it's in whitelist, apply the interpolator
 * 3. Not in blacklist or whitelist -> don't apply
 */
public abstract class BlackWhiteListInterpolator implements QueryInterpolator
{
  private final Set<String> whitelist;
  private final Set<String> blacklist;

  public BlackWhiteListInterpolator(List<String> whitelist, List<String> blacklist)
  {
    if (whitelist == null || whitelist.isEmpty()) {
      this.whitelist = null;
    } else {
      this.whitelist = new HashSet<>(whitelist);
    }

    if (blacklist == null || blacklist.isEmpty()) {
      this.blacklist = null;
    } else {
      this.blacklist = new HashSet<>(blacklist);
    }
  }

  public boolean shouldApply(String datasource)
  {
    if ((blacklist != null) && blacklist.contains(datasource)) {
      return false;
    }
    if ((whitelist == null) || (whitelist.contains(datasource))) {
      return true;
    }
    return false;
  }

  public boolean shouldApply(Query query)
  {
    String datasource = query.getDataSource().getNames().get(0);
    return shouldApply(datasource);
  }

  @JsonProperty("blacklist")
  public List<String> getBlacklist()
  {
    return new ArrayList<>(blacklist);
  }

  @JsonProperty("whitelist")
  public List<String> getWhitelist()
  {
    return new ArrayList<>(whitelist);
  }
}
