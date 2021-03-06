/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.loading;

import com.metamx.common.MapUtils;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
*/
public class CacheTestSegmentLoader implements SegmentLoader
{
  @Override
  public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
  {
    Map<String, Object> loadSpec = segment.getLoadSpec();
    return new File(MapUtils.getString(loadSpec, "cacheDir")).exists();
  }

  @Override
  public Segment getSegment(final DataSegment segment) throws SegmentLoadingException
  {
    return new Segment()
    {
      @Override
      public String getIdentifier()
      {
        return segment.getIdentifier();
      }

      @Override
      public Interval getDataInterval()
      {
        return segment.getInterval();
      }

      @Override
      public QueryableIndex asQueryableIndex()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException
      {
      }
    };
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cleanup(DataSegment loadSpec) throws SegmentLoadingException
  {
  }
}
