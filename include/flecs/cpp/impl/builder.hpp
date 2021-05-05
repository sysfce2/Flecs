
namespace flecs 
{

template <typename ... Components>
inline query_builder_base<Components...>::operator query<Components ...>() const {
    ecs_query_t *query = *this;
    return flecs::query<Components...>(m_world, query);
}

template <typename ... Components>
inline query_builder<Components ...>::operator query<>() const {
    ecs_query_t *query = *this;
    return flecs::query<>(this->m_world, query);
}

template <typename ... Components>
inline query<Components ...> query_builder_base<Components...>::build() const {
    ecs_query_t *query = *this;
    return flecs::query<Components...>(m_world, query);
}

template <typename Base, typename ... Components>
inline Base& query_builder_i<Base, Components ...>::parent(const query_base& parent) {
    m_desc->parent = parent.c_ptr();
    return *static_cast<Base*>(this);
}

template <typename ... Components>    
template <typename Func>
inline system<Components ...> system_builder<Components...>::action(Func&& func) {
    flecs::entity_t system = build<action_invoker_t<Func>>(std::forward<Func>(func), false);
    return flecs::system<Components...>(m_world, system);
}

template <typename ... Components>    
template <typename Func>
inline system<Components ...> system_builder<Components...>::iter(Func&& func) {
    using Invoker = typename _::iter_invoker<
        typename std::decay<Func>::type, Components...>;
    flecs::entity_t system = build<Invoker>(std::forward<Func>(func), false);
    return flecs::system<Components...>(m_world, system);
}

template <typename ... Components>    
template <typename Func>
inline system<Components ...> system_builder<Components...>::each(Func&& func) {
    using Invoker = typename _::each_invoker<
        typename std::decay<Func>::type, Components...>;
    flecs::entity_t system = build<Invoker>(std::forward<Func>(func), true);
    return flecs::system<Components...>(m_world, system);
}

}
