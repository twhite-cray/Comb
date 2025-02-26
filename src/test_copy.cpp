//////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2018-2022, Lawrence Livermore National Security, LLC.
//
// Produced at the Lawrence Livermore National Laboratory
//
// LLNL-CODE-758885
//
// All rights reserved.
//
// This file is part of Comb.
//
// For details, see https://github.com/LLNL/Comb
// Please also see the LICENSE file for MIT license.
//////////////////////////////////////////////////////////////////////////////

#include "comb.hpp"

namespace COMB {


template < typename exec_type >
bool should_do_copy(ContextHolder<exec_type>& con_in,
                    COMB::AllocatorInfo& dst_aloc_in,
                    COMB::AllocatorInfo& src_aloc_in)
{
  return con_in.available()
      && (dst_aloc_in.available(COMB::AllocatorInfo::UseType::Mesh)
       || dst_aloc_in.available(COMB::AllocatorInfo::UseType::Buffer))
      && (src_aloc_in.available(COMB::AllocatorInfo::UseType::Mesh)
       || src_aloc_in.available(COMB::AllocatorInfo::UseType::Buffer))
      && dst_aloc_in.accessible(con_in.get())
      && src_aloc_in.accessible(con_in.get()) ;
}

template < typename exec_type >
void do_copy(ContextHolder<exec_type>& con_in,
             CommInfo& comminfo,
             COMB::AllocatorInfo& dst_aloc_in,
             COMB::AllocatorInfo& src_aloc_in,
             Timer& tm, IdxT num_vars, IdxT len, IdxT nrepeats)
{
  if (!should_do_copy(con_in, dst_aloc_in, src_aloc_in)) {
    return;
  }

  using con_type  = typename ContextHolder<exec_type>::context_type;
  using pol  = typename con_type::pol;

  ExecContext<pol>& con = con_in.get();

  COMB::Allocator& dst_aloc = dst_aloc_in.allocator();
  COMB::Allocator& src_aloc = src_aloc_in.allocator();


  CPUContext tm_con;
  tm.clear();

  char test_name[1024] = ""; snprintf(test_name, 1024, "memcpy %s dst %s src %s", pol::get_name(), dst_aloc.name(), src_aloc.name());
  fgprintf(FileGroup::all, "Starting test %s\n", test_name);

  char sub_test_name[1024] = ""; snprintf(sub_test_name, 1024, "copy_sync-%d-%d-%zu", num_vars, len, sizeof(DataT));

  Range r(test_name, Range::green);

  DataT** src = new DataT*[num_vars];
  DataT** dst = new DataT*[num_vars];

  for (IdxT i = 0; i < num_vars; ++i) {
    src[i] = (DataT*)src_aloc.allocate(len*sizeof(DataT));
    dst[i] = (DataT*)dst_aloc.allocate(len*sizeof(DataT));
  }

  // setup
  for (IdxT i = 0; i < num_vars; ++i) {
    con.for_all(len, detail::set_n1{dst[i]});
    con.for_all(len, detail::set_0{src[i]});
    con.for_all(len, detail::set_copy{dst[i], src[i]});
  }

  con.synchronize();

  auto g1 = con.create_group();
  auto g2 = con.create_group();
  auto c1 = con.create_component();
  auto c2 = con.create_component();

  IdxT ntestrepeats = std::max(IdxT{1}, nrepeats/IdxT{10});
  for (IdxT rep = 0; rep < ntestrepeats; ++rep) { // test comm

    con.start_group(g1);
    con.start_component(g1, c1);
    for (IdxT i = 0; i < num_vars; ++i) {
      con.for_all(len, detail::set_copy{src[i], dst[i]});
    }
    con.finish_component(g1, c1);
    con.finish_group(g1);

    con.synchronize();

    // tm.start(tm_con, sub_test_name);

    con.start_group(g2);
    con.start_component(g2, c2);
    for (IdxT i = 0; i < num_vars; ++i) {
      con.for_all(len, detail::set_copy{dst[i], src[i]});
    }
    con.finish_component(g2, c2);
    con.finish_group(g2);

    con.synchronize();

    // tm.stop(tm_con);
  }

  for (IdxT rep = 0; rep < nrepeats; ++rep) {

    con.start_group(g1);
    con.start_component(g1, c1);
    for (IdxT i = 0; i < num_vars; ++i) {
      con.for_all(len, detail::set_copy{src[i], dst[i]});
    }
    con.finish_component(g1, c1);
    con.finish_group(g1);

    con.synchronize();

    tm.start(tm_con, sub_test_name);

    con.start_group(g2);
    con.start_component(g2, c2);
    for (IdxT i = 0; i < num_vars; ++i) {
      con.for_all(len, detail::set_copy{dst[i], src[i]});
    }
    con.finish_component(g2, c2);
    con.finish_group(g2);

    con.synchronize();

    tm.stop(tm_con);
  }

  con.destroy_component(c2);
  con.destroy_component(c1);
  con.destroy_group(g2);
  con.destroy_group(g1);

  print_timer(comminfo, tm);
  tm.clear();

  for (IdxT i = 0; i < num_vars; ++i) {
    dst_aloc.deallocate(dst[i]);
    src_aloc.deallocate(src[i]);
  }

  delete[] dst;
  delete[] src;
}

void test_copy_allocator(CommInfo& comminfo,
                         COMB::Executors& exec,
                         AllocatorInfo& dst_aloc,
                         AllocatorInfo& cpu_src_aloc,
                         AllocatorInfo& gpu_src_aloc,
                         Timer& tm, IdxT num_vars, IdxT len, IdxT nrepeats)
{
  char name[1024] = ""; snprintf(name, 1024, "set_vars %s", dst_aloc.allocator().name());
  Range r0(name, Range::green);

  do_copy(exec.seq, comminfo, dst_aloc, cpu_src_aloc, tm, num_vars, len, nrepeats);

#ifdef COMB_ENABLE_OPENMP
  do_copy(exec.omp, comminfo, dst_aloc, cpu_src_aloc, tm, num_vars, len, nrepeats);
#endif

#ifdef COMB_ENABLE_CUDA
  do_copy(exec.cuda, comminfo, dst_aloc, gpu_src_aloc, tm, num_vars, len, nrepeats);

#ifdef COMB_ENABLE_CUDA_GRAPH
  do_copy(exec.cuda_graph, comminfo, dst_aloc, gpu_src_aloc, tm, num_vars, len, nrepeats);
#endif
#else
  COMB::ignore_unused(gpu_src_aloc);
#endif

#ifdef COMB_ENABLE_HIP
  do_copy(exec.hip, comminfo, dst_aloc, gpu_src_aloc, tm, num_vars, len, nrepeats);
#else
  COMB::ignore_unused(gpu_src_aloc);
#endif

#ifdef COMB_ENABLE_RAJA
  do_copy(exec.raja_seq, comminfo, dst_aloc, cpu_src_aloc, tm, num_vars, len, nrepeats);

#ifdef COMB_ENABLE_OPENMP
  do_copy(exec.raja_omp, comminfo, dst_aloc, cpu_src_aloc, tm, num_vars, len, nrepeats);
#endif

#ifdef COMB_ENABLE_CUDA
  do_copy(exec.raja_cuda, comminfo, dst_aloc, gpu_src_aloc, tm, num_vars, len, nrepeats);
#else
  COMB::ignore_unused(gpu_src_aloc);
#endif

#ifdef COMB_ENABLE_HIP
  do_copy(exec.raja_hip, comminfo, dst_aloc, gpu_src_aloc, tm, num_vars, len, nrepeats);
#else
  COMB::ignore_unused(gpu_src_aloc);
#endif
#endif
}

void test_copy_allocators(CommInfo& comminfo,
                          COMB::Executors& exec,
                          COMB::Allocators& alloc,
                          AllocatorInfo& cpu_src_aloc,
                          AllocatorInfo& gpu_src_aloc,
                          Timer& tm, IdxT num_vars, IdxT len, IdxT nrepeats)
{
  test_copy_allocator(comminfo,
                      exec,
                      alloc.host,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

#ifdef COMB_ENABLE_CUDA

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_hostpinned,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_device,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_managed,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_managed_host_preferred,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_managed_host_preferred_device_accessed,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_managed_device_preferred,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.cuda_managed_device_preferred_host_accessed,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

#endif // COMB_ENABLE_CUDA

#ifdef COMB_ENABLE_HIP

  test_copy_allocator(comminfo,
                      exec,
                      alloc.hip_hostpinned,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.hip_device,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

  test_copy_allocator(comminfo,
                      exec,
                      alloc.hip_managed,
                      cpu_src_aloc,
                      gpu_src_aloc,
                      tm, num_vars, len, nrepeats);

#endif // COMB_ENABLE_HIP

}

void test_copy(CommInfo& comminfo,
               COMB::Executors& exec,
               COMB::Allocators& alloc,
               Timer& tm, IdxT num_vars, IdxT len, IdxT nrepeats)
{

  {
    // src host memory tests
    AllocatorInfo& cpu_src_aloc = alloc.host;

#if defined(COMB_ENABLE_CUDA)
    AllocatorInfo& gpu_src_aloc = alloc.cuda_hostpinned;
#elif defined(COMB_ENABLE_HIP)
    AllocatorInfo& gpu_src_aloc = alloc.hip_hostpinned;
#else
    AllocatorInfo& gpu_src_aloc = alloc.invalid;
#endif

    test_copy_allocators(comminfo,
                         exec,
                         alloc,
                         cpu_src_aloc,
                         gpu_src_aloc,
                         tm, num_vars, len, nrepeats);

  }

#ifdef COMB_ENABLE_CUDA
  {
    // src cuda memory tests
    AllocatorInfo& cpu_src_aloc = alloc.cuda_device;

    AllocatorInfo& gpu_src_aloc = alloc.cuda_device;

    test_copy_allocators(comminfo,
                         exec,
                         alloc,
                         cpu_src_aloc,
                         gpu_src_aloc,
                         tm, num_vars, len, nrepeats);

  }
#endif // COMB_ENABLE_CUDA

#ifdef COMB_ENABLE_HIP
  {
    // src hip memory tests
    AllocatorInfo& cpu_src_aloc = alloc.hip_device;

    AllocatorInfo& gpu_src_aloc = alloc.hip_device;

    test_copy_allocators(comminfo,
                         exec,
                         alloc,
                         cpu_src_aloc,
                         gpu_src_aloc,
                         tm, num_vars, len, nrepeats);

  }
#endif // COMB_ENABLE_HIP

}

} // namespace COMB
