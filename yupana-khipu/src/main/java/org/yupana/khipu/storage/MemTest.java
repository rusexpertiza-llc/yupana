/*
 * Copyright 2019 Rusexpertiza LLC
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

package org.yupana.khipu.storage;

public class MemTest {

    public static void main(String args[]) {
        int N = 5;
        int K = 20;
        int M = 100000000;

        long s = 0L;

        long[] arr = new long[M + N];

        int i = 0;

        while (i < M + N) {
            arr[i] = i;
            i ++;
        }


        i = 0;
        while (i < K) {
            long start = System.currentTimeMillis();
            int m = 0;
            while (m < M) {
                int j = 0;
                while (j < N) {
                    s += arr[m + j];
                    j ++;
                }
                m ++;
            }
            long ms = (System.currentTimeMillis() - start);
            double gbs = ((double) N * M * 8) / ms * 1000 / 1024 / 1024 / 1024;
            System.out.println("time: " +  ms + " ms, throughput: " + gbs + " GB/S " );
            i ++;
        }
        System.out.println(s);

        int N2 = N * M;
        long[] arr2 = new long[N2];

        i = 0;
        while (i < N2) {
            arr2[i] = i;
            i ++;
        }

        s = 0;
        i = 0;
        while (i < K) {
            long start = System.currentTimeMillis();
            int j = 0;
            while (j < N2) {
                s += arr2[j];
                j++;
            }
            i ++;
            long ms = (System.currentTimeMillis() - start);
            double gbs = ((double) N * M * 8) / ms * 1000 / 1024 / 1024 / 1024;
            System.out.println("flat time: " +  ms + " ms, throughput: " + gbs + " GB/S " );

        }
        System.out.println(s);

    }
}
