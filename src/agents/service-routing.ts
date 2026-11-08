import { stableJsonHash } from "../core/stable-hash.js";

/**
 * Rendezvous routing: the greatest SHA-256 tuple score wins. Route order is
 * irrelevant; adding/removing a route only remaps keys won by that route.
 * Lexical route ID breaks score ties deterministically. Config validation is
 * the caller's responsibility; this primitive requires a key and a candidate.
 */
export function selectStickyRoute(routingKey: string, routes: readonly string[]): string {
  if (routingKey.length === 0) throw new Error("Sticky routing key must not be empty");
  if (routes.length === 0) throw new Error("Sticky routing requires at least one route");
  let winner = routes[0]!;
  let winningScore = stableJsonHash([routingKey, winner]);
  for (let index = 1; index < routes.length; index++) {
    const route = routes[index]!;
    const score = stableJsonHash([routingKey, route]);
    if (score > winningScore || (score === winningScore && route > winner)) {
      winner = route;
      winningScore = score;
    }
  }
  return winner;
}
