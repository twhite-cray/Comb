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

#ifndef _COMM_POL_MPI_ACTIVE_RMA_HPP
#define _COMM_POL_MPI_ACTIVE_RMA_HPP

#include "config.hpp"

#ifdef COMB_ENABLE_MPI

#include "exec.hpp"
#include "comm_utils_mpi.hpp"
#include "MessageBase.hpp"
#include "ExecContext.hpp"

struct mpi_active_rma_pol {
  // static const bool async = false;
  static const bool mock = false;
  // compile mpi_type packing/unpacking tests for this comm policy
  static const bool use_mpi_type = false;
  static const bool persistent = true;
  static const char* get_name() { return "mpi_active_rma"; }
  using send_request_type = MPI_Request;
  using recv_request_type = MPI_Request;
  using send_status_type = MPI_Status;
  using recv_status_type = MPI_Status;
};

template < >
struct CommContext<mpi_active_rma_pol> : MPIContext
{
  using base = MPIContext;

  using pol = mpi_active_rma_pol;

  using send_request_type = typename pol::send_request_type;
  using recv_request_type = typename pol::recv_request_type;
  using send_status_type = typename pol::send_status_type;
  using recv_status_type = typename pol::recv_status_type;

  MPI_Comm comm = MPI_COMM_NULL;
  MPI_Group recv_group, send_group;
  MPI_Win recv_win, send_win;
  std::vector<int> recv_ranks, send_ranks;
  std::vector<long> offsets;

  CommContext()
    : base()
  { }

  CommContext(base const& b)
    : base(b)
  { }

  CommContext(CommContext const& a_, MPI_Comm comm_)
    : base(a_)
    , comm(comm_)
  { }

  void ensure_waitable()
  {

  }

  template < typename context >
  void waitOn(context& con)
  {
    con.ensure_waitable();
    base::waitOn(con);
  }

  send_request_type send_request_null() { return MPI_REQUEST_NULL; }
  recv_request_type recv_request_null() { return MPI_REQUEST_NULL; }
  send_status_type send_status_null() { return send_status_type{}; }
  recv_status_type recv_status_null() { return recv_status_type{}; }

  void connect_ranks(std::vector<int> const& send_ranks_,
                     std::vector<int> const& recv_ranks_)
  {
    send_ranks = send_ranks_;
    recv_ranks = recv_ranks_;
    MPI_Group world;
    MPI_Comm_group(comm, &world);
    MPI_Group_incl(world, recv_ranks.size(), recv_ranks.data(), &recv_group);
    MPI_Group_incl(world, send_ranks.size(), send_ranks.data(), &send_group);
  }

  void disconnect_ranks(std::vector<int> const& send_ranks_,
                        std::vector<int> const& recv_ranks_)
  {
    COMB::ignore_unused(send_ranks_, recv_ranks_);
    MPI_Group_free(&send_group);
    MPI_Group_free(&recv_group);
    recv_ranks.clear();
    send_ranks.clear();
  }


  void setup_mempool(COMB::Allocator& many_aloc,
                     COMB::Allocator& few_aloc)
  {
    COMB::ignore_unused(many_aloc, few_aloc);
  }

  void teardown_mempool()
  {
  }
};


namespace detail {

template < >
struct Message<MessageBase::Kind::send, mpi_active_rma_pol>
  : MessageInterface<MessageBase::Kind::send, mpi_active_rma_pol>
{
  using base = MessageInterface<MessageBase::Kind::send, mpi_active_rma_pol>;

  using policy_comm = typename base::policy_comm;
  using communicator_type = typename base::communicator_type;
  using request_type      = typename base::request_type;
  using status_type       = typename base::status_type;

  // use the base class constructor
  using base::base;

  static int wait_send_any(communicator_type&,
                           int count, request_type* requests,
                           status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::wait_send_any count %d requests %p statuses %p\n", count, requests, statuses);
    assert(!"supported");
    return -1;
  }

  static int test_send_any(communicator_type&,
                           int count, request_type* requests,
                           status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::test_send_any count %d requests %p statuses %p\n", count, requests, statuses);
    assert(!"supported");
    return -1;
  }

  static int wait_send_some(communicator_type&,
                            int count, request_type* requests,
                            int* indices, status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::wait_send_some count %d requests %p indices %p statuses %p\n", count, requests, indices, statuses);
    assert(!"supported");
    return -1;
  }

  static int test_send_some(communicator_type&,
                            int count, request_type* requests,
                            int* indices, status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::test_send_some count %d requests %p indices %p statuses %p\n", count, requests, indices, statuses);
    assert(!"supported");
    return -1;
  }

  static void wait_send_all(communicator_type&,
                            int count, request_type* requests,
                            status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::wait_send_all count %d requests %p statuses %p\n", count, requests, statuses);
    LOGPRINTF("Message<mpi>::wait_send_all return\n");
  }

  static bool test_send_all(communicator_type&,
                            int count, request_type* requests,
                            status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::test_send_all count %d requests %p statuses %p\n", count, requests, statuses);
    assert(!"supported");
    return -1;
  }
};


template < >
struct Message<MessageBase::Kind::recv, mpi_active_rma_pol>
  : MessageInterface<MessageBase::Kind::recv, mpi_active_rma_pol>
{
  using base = MessageInterface<MessageBase::Kind::recv, mpi_active_rma_pol>;

  using policy_comm = typename base::policy_comm;
  using communicator_type = typename base::communicator_type;
  using request_type      = typename base::request_type;
  using status_type       = typename base::status_type;

  // use the base class constructor
  using base::base;


  static int wait_recv_any(communicator_type&,
                           int count, request_type* requests,
                           status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::wait_recv_any count %d requests %p statuses %p\n", count, requests, statuses);
    assert(!"supported");
    return -1;
  }

  static int test_recv_any(communicator_type&,
                           int count, request_type* requests,
                           status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::test_recv_any count %d requests %p statuses %p\n", count, requests, statuses);
    assert(!"supported");
    return -1;
  }

  static int wait_recv_some(communicator_type&,
                            int count, request_type* requests,
                            int* indices, status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::wait_recv_some count %d requests %p indices %p statuses %p\n", count, requests, indices, statuses);
    assert(!"supported");
    return -1;
  }

  static int test_recv_some(communicator_type&,
                            int count, request_type* requests,
                            int* indices, status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::test_recv_some count %d requests %p indices %p statuses %p\n", count, requests, indices, statuses);
    assert(!"supported");
    return -1;
  }

  static void wait_recv_all(communicator_type &comm,
                            int count, request_type* requests,
                            status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::wait_recv_all count %d requests %p statuses %p\n", count, requests, statuses);
    MPI_Win_complete(comm.recv_win);
    MPI_Win_wait(comm.recv_win);
    LOGPRINTF("Message<mpi>::wait_recv_all return\n");
  }

  static bool test_recv_all(communicator_type&,
                            int count, request_type* requests,
                            status_type* statuses)
  {
    LOGPRINTF("Message<mpi>::test_recv_all count %d requests %p statuses %p\n", count, requests, statuses);
    assert(!"supported");
    return -1;
  }
};


__attribute__((unused))
static size_t page_align(const size_t n)
{ 
  constexpr size_t page = 4096;
  return ((n + page - 1) / page) * page;
}


template < typename exec_policy >
struct MessageGroup<MessageBase::Kind::send, mpi_active_rma_pol, exec_policy>
  : detail::MessageGroupInterface<MessageBase::Kind::send, mpi_active_rma_pol, exec_policy>
{
  using base = detail::MessageGroupInterface<MessageBase::Kind::send, mpi_active_rma_pol, exec_policy>;

  using policy_comm       = typename base::policy_comm;
  using communicator_type = typename base::communicator_type;
  using message_type      = typename base::message_type;
  using request_type      = typename base::request_type;
  using status_type       = typename base::status_type;

  using message_item_type = typename base::message_item_type;
  using context_type      = typename base::context_type;
  using event_type        = typename base::event_type;
  using group_type        = typename base::group_type;
  using component_type    = typename base::component_type;

  // use the base class constructor
  using base::base;


  void finalize()
  {
    // call base finalize
    base::finalize();
  }

  void setup(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, request_type* requests)
  {
    COMB::ignore_unused(con, con_comm, msgs, len, requests);
  }

  void cleanup(communicator_type& con_comm, message_type** msgs, IdxT len, request_type* requests)
  {
    COMB::ignore_unused(con_comm, msgs, len, requests);
  }

  void allocate(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    COMB::ignore_unused(con, async);
    LOGPRINTF("%p send allocate msgs %p len %d\n", this, msgs, len);
    if (len <= 0) return;
   
    const size_t var_size = this->m_variables.size();
    size_t nbytes = 0;
    for (IdxT i = 0; i < len; ++i) {
      message_type *const msg = msgs[i];
      assert(msg->buf == nullptr);
      nbytes += page_align(msg->nbytes() * var_size);
    }

    msgs[0]->buf = this->m_aloc.allocate(nbytes);
    LOGPRINTF("%p send allocate %d msgs %p buf %p nbytes %lu\n", this, len, msgs[0], msgs[0]->buf, nbytes);

    for (IdxT i = 1; i < len; i++) {
      message_type *const prev = msgs[i-1];
      msgs[i]->buf = reinterpret_cast<char*>(prev->buf) + page_align(prev->nbytes() * var_size);
    }

    MPI_Win_create(msgs[0]->buf, nbytes, 1, MPI_INFO_NULL, con_comm.comm, &con_comm.send_win);

    if (comb_allow_pack_loop_fusion()) {
      this->m_fuser.allocate(con, this->m_variables, this->m_items.size());
    }
  }

  void pack(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    COMB::ignore_unused(con_comm);
    LOGPRINTF("%p send pack con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;
    con.start_group(this->m_groups[len-1]);
    if (!comb_allow_pack_loop_fusion()) {
      for (IdxT i = 0; i < len; ++i) {
        const message_type* msg = msgs[i];
        LOGPRINTF("%p send pack msg %p buf %p\n", this, msg, msg->buf);
        const IdxT msg_idx = msg->idx;
        char* buf = static_cast<char*>(msg->buf);
        assert(buf != nullptr);
        this->m_contexts[msg_idx].start_component(this->m_groups[len-1], this->m_components[msg_idx]);
        for (const MessageItemBase* msg_item : msg->message_items) {
          const message_item_type* item = static_cast<const message_item_type*>(msg_item);
          LOGPRINTF("%p send pack con %p item %p buf %p = srcs[indices %p] nitems %d\n", this, &this->m_contexts[msg_idx], item, buf, item->indices, item->size);
          const IdxT nitems = item->size;
          const IdxT nbytes = item->nbytes;
          LidxT const* indices = item->indices;
          for (DataT const* src : this->m_variables) {
            this->m_contexts[msg_idx].for_all(nitems, make_copy_idxr_idxr(src, detail::indexer_list_i{indices},
                                               static_cast<DataT*>(static_cast<void*>(buf)), detail::indexer_i{}));
            buf += nbytes;
          }
        }
        if (async == detail::Async::no) {
          this->m_contexts[msg_idx].finish_component(this->m_groups[len-1], this->m_components[msg_idx]);
        } else {
          this->m_contexts[msg_idx].finish_component_recordEvent(this->m_groups[len-1], this->m_components[msg_idx], this->m_events[msg_idx]);
        }
      }
    }
    else if (async == detail::Async::no) {
      for (IdxT i = 0; i < len; ++i) {
        const message_type* msg = msgs[i];
        LOGPRINTF("%p send pack msg %p buf %p\n", this, msg, msg->buf);
        char* buf = static_cast<char*>(msg->buf);
        assert(buf != nullptr);
        for (const MessageItemBase* msg_item : msg->message_items) {
          const message_item_type* item = static_cast<const message_item_type*>(msg_item);
          LOGPRINTF("%p send pack con %p item %p buf %p = srcs[indices %p] nitems %d\n", this, &con, item, buf, item->indices, item->size);
          this->m_fuser.enqueue(con, (DataT*)buf, item->indices, item->size);
          buf += item->nbytes * this->m_variables.size();
          assert(static_cast<IdxT>(item->size*sizeof(DataT)) == item->nbytes);
        }
      }
      this->m_fuser.exec(con);
    } else {
      for (IdxT i = 0; i < len; ++i) {
        const message_type* msg = msgs[i];
        LOGPRINTF("%p send pack msg %p buf %p\n", this, msg, msg->buf);
        const IdxT msg_idx = msg->idx;
        char* buf = static_cast<char*>(msg->buf);
        assert(buf != nullptr);
        this->m_contexts[msg_idx].start_component(this->m_groups[len-1], this->m_components[msg_idx]);
        for (const MessageItemBase* msg_item : msg->message_items) {
          const message_item_type* item = static_cast<const message_item_type*>(msg_item);
          LOGPRINTF("%p send pack con %p item %p buf %p = srcs[indices %p] nitems %d\n", this, &this->m_contexts[msg_idx], item, buf, item->indices, item->size);
          this->m_fuser.enqueue(this->m_contexts[msg_idx], (DataT*)buf, item->indices, item->size);
          buf += item->nbytes * this->m_variables.size();
          assert(static_cast<IdxT>(item->size*sizeof(DataT)) == item->nbytes);
        }
        this->m_fuser.exec(this->m_contexts[msg_idx]);
        this->m_contexts[msg_idx].finish_component_recordEvent(this->m_groups[len-1], this->m_components[msg_idx], this->m_events[msg_idx]);
      }
    }
    con.finish_group(this->m_groups[len-1]);
  }

  IdxT wait_pack_complete(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    LOGPRINTF("%p send wait_pack_complete con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return 0;
    if (async == detail::Async::no) {
      con_comm.waitOn(con);
    } else {
      for (IdxT i = 0; i < len; ++i) {
        const message_type* msg = msgs[i];
        if (!this->m_contexts[msg->idx].queryEvent(this->m_events[msg->idx])) {
          return i;
        }
      }
    }
    return len;
  }

  static void start_Isends(context_type& con, communicator_type& con_comm)
  {
    LOGPRINTF("send start_Isends con %p\n", &con);
    COMB::ignore_unused(con, con_comm);
  }

  void Isend(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async, request_type* requests)
  {
    COMB::ignore_unused(async, requests);
    LOGPRINTF("%p send Isend con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;
    start_Isends(con, con_comm);
    MPI_Win_start(con_comm.send_group, 0, con_comm.recv_win);
    for (IdxT i = 0; i < len; ++i) {
      const message_type* msg = msgs[i];
      LOGPRINTF("%p send Put msg %p buf %p nbytes %d to %i\n",
                this, msg, msg->buf, msg->nbytes() * this->m_variables.size(), msg->partner_rank);
      char* buf = static_cast<char*>(msg->buf);
      assert(buf != nullptr);
      const int partner_rank = msg->partner_rank;
      const IdxT msg_nbytes = msg->nbytes() * this->m_variables.size();
      MPI_Put(buf, msg_nbytes, MPI_BYTE, partner_rank, con_comm.offsets[i], msg_nbytes, MPI_BYTE, con_comm.recv_win);
    }
    finish_Isends(con, con_comm);
  }

  static void finish_Isends(context_type& con, communicator_type& con_comm)
  {
    LOGPRINTF("send finish_Isends con %p\n", &con);
    COMB::ignore_unused(con, con_comm);
  }

  void deallocate(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    COMB::ignore_unused(con, async);
    LOGPRINTF("%p send deallocate con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;

    MPI_Win_free(&con_comm.send_win);

    LOGPRINTF("%p send deallocate %d msgs %p buf %p\n", this, len, msgs[0], msgs[0]->buf);
    this->m_aloc.deallocate(msgs[0]->buf);

    for (IdxT i = 0; i < len; ++i) {
      message_type* msg = msgs[i];
      assert(msg->buf != nullptr);
      msg->buf = nullptr;
    }

    if (comb_allow_pack_loop_fusion()) {
      this->m_fuser.deallocate(con);
    }
  }
};

template < typename exec_policy >
struct MessageGroup<MessageBase::Kind::recv, mpi_active_rma_pol, exec_policy>
  : detail::MessageGroupInterface<MessageBase::Kind::recv, mpi_active_rma_pol, exec_policy>
{
  using base = detail::MessageGroupInterface<MessageBase::Kind::recv, mpi_active_rma_pol, exec_policy>;

  using policy_comm       = typename base::policy_comm;
  using communicator_type = typename base::communicator_type;
  using message_type      = typename base::message_type;
  using request_type      = typename base::request_type;
  using status_type       = typename base::status_type;

  using message_item_type = typename base::message_item_type;
  using context_type      = typename base::context_type;
  using event_type        = typename base::event_type;
  using group_type        = typename base::group_type;
  using component_type    = typename base::component_type;

  // use the base class constructor
  using base::base;


  void finalize()
  {
    // call base finalize
    base::finalize();
  }

  void setup(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, request_type* requests)
  {
    COMB::ignore_unused(con, con_comm, msgs, len, requests);
  }

  void cleanup(communicator_type& con_comm, message_type** msgs, IdxT len, request_type* requests)
  {
    COMB::ignore_unused(con_comm, msgs, len, requests);
  }

  void allocate(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    COMB::ignore_unused(con, async);
    LOGPRINTF("%p recv allocate con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;
    
    std::vector<long> offsets(len);
    const size_t var_size = this->m_variables.size();
    long nbytes = 0;
    for (IdxT i = 0; i < len; ++i) {
      message_type *const msg = msgs[i];
      assert(msg->buf == nullptr);
      offsets[i] = nbytes;
      nbytes += page_align(msg->nbytes() * var_size);
    }

    char *const buf = reinterpret_cast<char*>(this->m_aloc.allocate(nbytes));
    for (IdxT i = 0; i < len; i++) msgs[i]->buf = buf + offsets[i];
    LOGPRINTF("%p recv allocate %d msgs %p buf %p nbytes %lu\n", this, len, msgs[0], msgs[0]->buf, nbytes);

    MPI_Win_create(msgs[0]->buf, nbytes, 1, MPI_INFO_NULL, con_comm.comm, &con_comm.recv_win);

    {
      constexpr int tag = 33;
      const int send_size = con_comm.send_ranks.size();
      const int recv_size = con_comm.recv_ranks.size();
      const int reqs_size = send_size + recv_size;
      std::vector<MPI_Request> reqs(reqs_size);

      // Recv window offsets from future receivers
      con_comm.offsets.resize(send_size);
      for (int i = 0; i < send_size; i++) MPI_Irecv(&con_comm.offsets[i], 1, MPI_LONG, con_comm.send_ranks[i], tag, con_comm.comm, &reqs[i]);

      // Send window offsets to future senders
      for (int i = 0; i < recv_size; i++) MPI_Isend(&offsets[i], 1, MPI_LONG, con_comm.recv_ranks[i], tag, con_comm.comm, &reqs[i + send_size]);

      MPI_Waitall(reqs_size, reqs.data(), MPI_STATUSES_IGNORE);
    }

    if (comb_allow_pack_loop_fusion()) {
      this->m_fuser.allocate(con, this->m_variables, this->m_items.size());
    }
  }

  void Irecv(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async, request_type* requests)
  {
    COMB::ignore_unused(con, msgs, len, async, requests);
    LOGPRINTF("%p recv Irecv con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;
    MPI_Win_post(con_comm.recv_group, 0, con_comm.recv_win);
  }

  void unpack(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    COMB::ignore_unused(con_comm, async);
    LOGPRINTF("%p recv unpack con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;
    con.start_group(this->m_groups[len-1]);
    if (!comb_allow_pack_loop_fusion()) {
      for (IdxT i = 0; i < len; ++i) {
        const message_type* msg = msgs[i];
        LOGPRINTF("%p recv unpack msg %p buf %p\n", this, msg, msg->buf);
        const IdxT msg_idx = msg->idx;
        char const* buf = static_cast<char const*>(msg->buf);
        assert(buf != nullptr);
        this->m_contexts[msg_idx].start_component(this->m_groups[len-1], this->m_components[msg_idx]);
        for (const MessageItemBase* msg_item : msg->message_items) {
          const message_item_type* item = static_cast<const message_item_type*>(msg_item);
          LOGPRINTF("%p recv unpack con %p item %p dsts[indices %p] = buf %p[i] nitems %d\n", this, &this->m_contexts[msg_idx], item, item->indices, buf, item->size);
          const IdxT nitems = item->size;
          const IdxT nbytes = item->nbytes;
          LidxT const* indices = item->indices;
          for (DataT* dst : this->m_variables) {

            // if (nitems*sizeof(DataT) == nbytes) {
            //   LOGPRINTF("  buf %p nitems %d nbytes %d [ ", buf, nitems, nbytes);
            //   for (IdxT idx = 0; idx < nitems; ++idx) {
            //     LOGPRINTF("%f ", (double)((DataT const*)buf)[idx]);
            //   }
            //   LOGPRINTF("] dst %p[indices %p]\n", dst, indices);
            // }

            this->m_contexts[msg_idx].for_all(nitems, make_copy_idxr_idxr(static_cast<DataT const*>(static_cast<void const*>(buf)), detail::indexer_i{},
                                               dst, detail::indexer_list_i{indices}));
            buf += nbytes;
          }
        }
        this->m_contexts[msg_idx].finish_component(this->m_groups[len-1], this->m_components[msg_idx]);
      }
    }
    else {
      for (IdxT i = 0; i < len; ++i) {
        const message_type* msg = msgs[i];
        LOGPRINTF("%p recv unpack msg %p buf %p\n", this, msg, msg->buf);
        char const* buf = static_cast<char const*>(msg->buf);
        assert(buf != nullptr);
        for (const MessageItemBase* msg_item : msg->message_items) {
          const message_item_type* item = static_cast<const message_item_type*>(msg_item);
          LOGPRINTF("%p recv unpack con %p item %p dsts[indices %p] = buf %p[i] nitems %d\n", this, &con, item, item->indices, buf, item->size);
          const IdxT nitems = item->size;
          const IdxT nbytes = item->nbytes;
          LidxT const* indices = item->indices;

          // for (DataT* dst : this->m_variables) {
          //   char const* print_buf = buf;
          //   if (nitems*sizeof(DataT) == nbytes) {
          //     LOGPRINTF("  buf %p nitems %d nbytes %d [ ", print_buf, nitems, nbytes);
          //     for (IdxT idx = 0; idx < nitems; ++idx) {
          //       LOGPRINTF("%f ", (double)((DataT const*)print_buf)[idx]);
          //     }
          //     LOGPRINTF("] dst %p[indices %p]\n", dst, indices);
          //   }
          //   print_buf += nbytes;
          // }

          this->m_fuser.enqueue(con, (DataT const*)buf, indices, nitems);
          buf += nbytes * this->m_variables.size();
          assert(static_cast<IdxT>(nitems*sizeof(DataT)) == nbytes);
        }
      }
      this->m_fuser.exec(con);
    }
    con.finish_group(this->m_groups[len-1]);
  }

  void deallocate(context_type& con, communicator_type& con_comm, message_type** msgs, IdxT len, detail::Async async)
  {
    COMB::ignore_unused(con, async);
    LOGPRINTF("%p recv deallocate con %p msgs %p len %d\n", this, &con, msgs, len);
    if (len <= 0) return;

    MPI_Win_free(&con_comm.recv_win);

    LOGPRINTF("%p recv deallocate %d msgs %p buf %p\n", this, len, msgs[0], msgs[0]->buf);
    this->m_aloc.deallocate(msgs[0]->buf);

    for (IdxT i = 0; i < len; ++i) {
      message_type* msg = msgs[i];
      assert(msg->buf != nullptr);
      msg->buf = nullptr;
    }

    if (comb_allow_pack_loop_fusion()) {
      this->m_fuser.deallocate(con);
    }
  }
};

} // namespace detail

#endif

#endif // _COMM_POL_MPI_ACTIVE_RMA_HPP
