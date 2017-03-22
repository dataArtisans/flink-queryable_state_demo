/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.queryablestatedemo;

/**
 * A bump event.
 */
public class BumpEvent {

  /**
   * ID of the user that triggered the bump.
   */
  private final int userId;

  /**
   * ID of the item that was bumped (article, post, etc.).
   */
  private final String itemId;

  public BumpEvent(int userId, String itemId) {
    this.userId = userId;
    this.itemId = itemId;
  }

  /**
   * Returns the ID of the user that triggered the bump.
   *
   * @return ID of the user that triggered the bump.
   */
  public int getUserId() {
    return userId;
  }

  /**
   * Returns the ID of the content that was bumped.
   *
   * @return ID of the content that was bumped.
   */
  public String getItemId() {
    return itemId;
  }

  @Override
  public String toString() {
    return "BumpEvent{" +
        "userId=" + userId +
        ", itemId=" + itemId +
        '}';
  }
}
