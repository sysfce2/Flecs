#include <union.h>
#include <iostream>

// This example shows how to use union relationships. Union relationships behave
// much like exclusive relationships in that entities can have only one instance
// and that adding an instance removes the previous instance.
//
// What makes union relationships stand out is that changing the relationship
// target doesn't change the archetype of an entity. This allows for quick
// switching of tags, which can be useful when encoding state machines in ECS.
//
// There is a tradeoff, and that is that because a single archetype can contain
// entities with multiple targets, queries need to do a bit of extra work to
// only return the requested target. 
//
// This code uses enumeration relationships. See the enum_relations example for
// more details.

enum Movement {
    Walking,
    Running
};

enum Direction {
    Front,
    Back,
    Left,
    Right
};

int main(int argc, char *argv[]) {
    flecs::world ecs(argc, argv);

    ecs.component<Movement>().add(flecs::Union);
    ecs.component<Direction>().add(flecs::Union);

    // Create a query that subscribes for all entities that have a Direction
    // and that are walking.
    flecs::query<> q = ecs.query_builder()
        .with(Walking).in()
        .with<Direction>(flecs::Wildcard)
        .build();

    // Create a few entities with various state combinations
    ecs.entity("e1")
        .add(Walking)
        .add(Front);

    ecs.entity("e2")
        .add(Running)
        .add(Left);

    flecs::entity e3 = ecs.entity("e3")
        .add(Running)
        .add(Back);

    // Add Walking to e3. This will remove the Running case
    e3.add(Walking);

    // Iterate the query
    q.each([&](const flecs::iter& it, size_t i) {
        flecs::entity e = it.entity(i);

        // Movement will always be Walking, Direction can be any state
        std::cout << e.name() 
            << ": Movement: " 
            << it.pair(0).second().name()
            << ", Direction: "
            << it.pair(1).second().name()
            << std::endl;
    });

    // Output:
    //   e3: Movement: Walking, Direction: Back
    //   e1: Movement: Walking, Direction: Front

    return 0;
}
